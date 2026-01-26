# Replication Module

This module manages the Master/Follower (Slave) replication logic, ensuring data consistency across multiple Carade nodes using an asynchronous replication model.

## Core Mechanics

### 1. Master/Slave Roles
*   **Master (Leader)**:
    *   Maintains a list of connected replicas (`CopyOnWriteArrayList<ClientHandler>`).
    *   Propagates all write commands (from `WriteSequencer`) to replicas.
    *   Maintains a `ReplicationBacklog` to support partial resynchronization (PSYNC).
*   **Slave (Follower)**:
    *   Connects to a Master node using a dedicated thread (`ReplicationLoop`).
    *   Performs an initial handshake (`PING`, `REPLCONF`).
    *   Requests synchronization (`PSYNC`).
    *   Receives and processes the command stream.

### 2. Handshake & Synchronization Flow
The replication handshake follows a strict sequence:
1.  **Connection**: Follower connects to Master's port.
2.  **PING**: Follower sends `PING` to verify liveness.
3.  **REPLCONF**: Follower announces its listening port (`REPLCONF listening-port <port>`).
4.  **PSYNC (Partial Sync)**: Follower requests `PSYNC <repl_id> <offset>` (currently simplified to `PSYNC ? -1` for initial sync).
5.  **Decision**:
    *   **Full Resync**: If offset is invalid or unknown, Master sends `+FULLRESYNC`, followed by the RDB Snapshot.
    *   **Partial Resync**: If offset is found in `ReplicationBacklog`, Master sends `+CONTINUE` and streams only the missing commands.

### 3. Replication Backlog
*   A **Fixed-Size Circular Buffer** (Ring Buffer).
*   Stores the most recent write commands in raw byte format.
*   **Logic**:
    *   New writes overwrite old data when the buffer is full.
    *   `isValidOffset(offset)` checks if the requested offset falls within `[globalOffset - size, globalOffset]`.
    *   Crucial for handling temporary network partitions without triggering an expensive Full Resync (RDB transfer).

### 4. Race Condition Handling
Replication is inherently prone to race conditions (e.g., writing to DB while calculating RDB offset).
*   **WriteSequencer**: Acts as the central serialization point. It ensures that for every write:
    1.  The DB is updated.
    2.  The command is written to the Backlog.
    3.  The command is propagated to Replicas.
*   This atomic sequence prevents "Split Brain" scenarios where the Backlog and the Dataset diverge.

## Technical Specifications

| Feature | Specification |
| :--- | :--- |
| **Protocol** | Redis Serialization Protocol (RESP) |
| **Sync Strategy** | Asynchronous (Replicas acknowledge implicitly) |
| **Backlog Type** | Circular Byte Buffer |
| **RDB Transport** | Streamed as a single Bulk String |
| **Failover** | Manual (No Sentinel/Cluster implementation yet) |

## Key Components

| Class | Responsibility |
| :--- | :--- |
| `ReplicationManager` | Singleton orchestrator. Manages Master/Slave state, threads, and peer lists. |
| `ReplicationBacklog` | Implements the circular buffer logic for Partial Resync. |
| `PsyncCommand` | Handles the `PSYNC` command on the Master side (determines Full vs Partial). |
| `WriteSequencer` | Feeds the replication stream. Ensures consistency between DB and Backlog. |

## Extension & Usage

*   **Setting up a Replica**:
    *   Command: `SLAVEOF <host> <port>`
    *   To promote to Master: `SLAVEOF NO ONE`
*   **Debugging**:
    *   Logs are prefixed with `🔗 Replication` (Slave) or standard Info logs (Master).
    *   Check `INFO REPLICATION` command output for offset details.
*   **Future Improvements**:
    *   Implement `REPLCONF ACK <offset>` for lag detection.
    *   Add Replication ID (RunID) support to allow Partial Resync after restart.
