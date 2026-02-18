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
