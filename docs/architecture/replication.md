# Replication

Carade supports **Master-Replica (Slave)** replication, allowing you to create exact copies of your database on multiple servers.

## Overview

Replication is implemented asynchronously.

*   **Master:** Handles all write operations and replicates them to connected slaves.
*   **Replica:** Replays commands received from the master to stay synchronized.

## Configuration

To set up replication:

**On Replica:**

```bash
# Connect to master at 127.0.0.1:63790
REPLICAOF 127.0.0.1 63790
```

Or configure in `carade.conf`:
```properties
replicaof 127.0.0.1 63790
masterauth teasertopsecret
```

## How It Works

1.  **Handshake:**
    *   Replica connects to Master.
    *   Sends `PING`.
    *   Authenticates with `AUTH` (if `masterauth` is set).
    *   Sends `REPLCONF listening-port <port>`.

2.  **Full Resynchronization (PSYNC):**
    *   If partial resync is not possible (new connection or backlog too old), Master creates an RDB snapshot.
    *   Master sends the snapshot to the Replica.
    *   Replica loads the RDB into memory.

3.  **Command Propagation:**
    *   After sync, Master forwards all write commands (e.g., `SET`, `DEL`, `expire`) to the Replica.
    *   Replica executes these commands locally.

## Partial Resync (PSYNC)

If the connection drops briefly, Carade attempts a **Partial Resynchronization**:

*   **Replication Backlog:** Master keeps a circular buffer of recent commands.
*   **Offset:** Replica sends its last processed offset (`PSYNC <replid> <offset>`).
*   **Catch Up:** If the offset is within the backlog, Master sends only the missing commands, avoiding a full RDB transfer.

## Building a Fault-Tolerant Caching Layer

When using Carade as a caching layer, you can leverage Master-Replica replication to improve read scalability and ensure high availability.

### Failover Logic Implementation

Carade currently does not have an automatic Sentinel built-in. Failover must be handled by your application or a proxy layer (like HAProxy or Envoy). Here is an example of handling failover at the application layer using Python:

```python
import redis
import time

# Define your master and replica nodes
master_pool = redis.ConnectionPool(host='10.0.0.1', port=63790, socket_timeout=2)
replica_pool = redis.ConnectionPool(host='10.0.0.2', port=63790, socket_timeout=2)

def execute_with_failover(command_type, command_func, *args, **kwargs):
    """
    Executes a command with manual failover.
    Writes MUST go to the master. Reads can go to the replica.
    """
    if command_type == 'write':
        try:
            r = redis.Redis(connection_pool=master_pool)
            return command_func(r, *args, **kwargs)
        except redis.exceptions.ConnectionError:
            print("CRITICAL: Master is down! Writes are failing.")
            # In a real system, you might trigger an alert or a script to promote the replica here
            raise
    
    elif command_type == 'read':
        try:
            # Prefer reading from the replica to offload the master
            r = redis.Redis(connection_pool=replica_pool)
            return command_func(r, *args, **kwargs)
        except redis.exceptions.ConnectionError:
            print("WARNING: Replica is down, falling back to Master for reads.")
            try:
                r = redis.Redis(connection_pool=master_pool)
                return command_func(r, *args, **kwargs)
            except redis.exceptions.ConnectionError:
                print("CRITICAL: Both Master and Replica are down!")
                raise

# Usage
def do_set(r, key, value):
    return r.set(key, value)

def do_get(r, key):
    return r.get(key)

# Write goes to master
execute_with_failover('write', do_set, 'cache_key', 'some_data')

# Read preferentially goes to replica
data = execute_with_failover('read', do_get, 'cache_key')
```

### Consistency Considerations

When designing your caching layer with replication, keep these consistency trade-offs in mind:

1.  **Asynchronous Replication Lag:** Because Carade's replication is asynchronous, there is a small window where a write executed on the Master has not yet reached the Replica.
2.  **Stale Reads:** If your application reads from a Replica immediately after writing to the Master, it might receive stale data (or no data) if the replication hasn't caught up.
3.  **Read-Your-Writes Consistency:** If your application requires strict read-your-writes consistency, you must either:
    *   Route reads for recently modified keys back to the Master for a short duration.
    *   Only read from the Master for critical paths.
4.  **Failover Data Loss:** If the Master fails catastrophically before sending its replication buffer to the Replica, any acknowledged but unreplicated writes will be lost when the Replica is promoted to the new Master.
