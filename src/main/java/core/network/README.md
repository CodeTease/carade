# Network Module

The `core.network` module handles all client communication, protocol parsing, and connection lifecycle management. It acts as the bridge between the raw TCP bytes and the command execution logic.

## Core Mechanics

### The Pipeline
Carade uses Netty for high-performance non-blocking I/O. The pipeline typically consists of:
1.  **Decoder:** `NettyRespDecoder` (in `core.protocol.netty`) reads raw bytes and assembles them into RESP (Redis Serialization Protocol) command objects (`List<byte[]>`).
2.  **Handler:** `ClientHandler` processes these command objects.

### Threading & Locking
To ensure data consistency while maintaining high concurrency, Carade employs a **Hybrid I/O + Global Lock** model:
*   **I/O Threads:** Netty handles reading/writing on multiple event loop threads.
*   **Execution:** Command execution is guarded by `Carade.globalRWLock` (ReentrantReadWriteLock).
    *   **Read Commands (GET, EXISTS):** Acquire a *Read Lock*, allowing parallel execution.
    *   **Write Commands (SET, DEL):** Acquire a *Write Lock*, ensuring exclusive access to the database.

### Transaction Management
The `ClientHandler` maintains the state for ACID transactions (`MULTI`/`EXEC`):
*   When `MULTI` is issued, `isInTransaction` is set to true.
*   Subsequent commands are queued in `transactionQueue` instead of being executed immediately.
*   `EXEC` executes the queued commands atomically (holding the global write lock).

## Technical Specifications

*   **Framework:** Netty.
*   **Protocol:** RESP (Redis Serialization Protocol). Supports Array, Bulk String, Integer, Error, and Simple String types.
*   **Timeout:** Supports blocking commands (`BLPOP`, `BRPOP`) with timeouts handled via Netty's scheduled executors.

## Key Components

| Class | Responsibility |
| :--- | :--- |
| `ClientHandler` | The primary Netty inbound handler. Manages user authentication, transaction state, and dispatches commands to `CommandRegistry`. |
| `NettyRespDecoder` | (Located in `core.protocol.netty`) Parses the incoming byte stream into discrete commands. |

## Extension & Usage

*   **Adding New Protocol Features:**
    *   Modify `NettyRespDecoder` to support new RESP data types (e.g., Maps, Sets in RESP3).
    *   Update `ClientHandler.send()` methods to serialize these new types.

*   **Integration:**
    *   `ClientHandler` implements `PubSub.Subscriber` to handle real-time message delivery.
    *   It interacts with `WriteSequencer` for strictly serialized write operations if enabled.
