# Server Core Module

This module defines the architectural backbone of Carade, including the threading model, network I/O, and the central coordination of write operations.

## Core Mechanics

### 1. Threading Model: "Hybrid I/O + Single Writer"
Carade adopts a hybrid threading model to balance performance and consistency:

*   **Multi-threaded I/O (Netty)**:
    *   Network connections, command parsing (RESP), and writing responses are handled by multiple threads (Netty `EventLoopGroup`).
    *   This allows Carade to handle thousands of concurrent connections efficiently.

*   **Single-Threaded Writer (Virtual/Logical)**:
    *   **All write operations are serialized**. Unlike typical multi-threaded Java applications that use fine-grained locking (per-key), Carade strictly orders all mutations to the global state.
    *   This mimics Redis's single-threaded nature, eliminating complex deadlock scenarios and ensuring deterministic execution order for Replication and AOF.

### 2. The Write Sequencer
The `WriteSequencer` class is the heart of this model. It acts as a gatekeeper for all data modification.

*   **Workflow**:
    1.  A Netty thread receives a write command (e.g., `SET key val`).
    2.  The thread submits the operation to the `WriteSequencer`.
    3.  `WriteSequencer` acquires the **Global Write Lock**.
    4.  **Critical Section**:
        *   Update In-Memory Database (`Carade.db`).
        *   Append to `ReplicationBacklog`.
        *   Log to AOF.
        *   Propagate to connected Replicas.
    5.  Release Lock.
    6.  Netty thread writes the response to the client.

*   **Design Consideration**:
    *   *Why not simple `synchronized` blocks?* We need to coordinate multiple subsystems (DB, Disk, Network) atomically. If we updated the DB but failed to log to AOF due to a race condition, the system would be inconsistent. The Sequencer enforces this atomic bundle.

## Technical Specifications

| Component | Specification |
| :--- | :--- |
| **Network Framework** | Netty 4.x (NIO) |
| **Protocol** | RESP (Redis Serialization Protocol) |
| **Concurrency Primitive** | `ReentrantReadWriteLock` (Global Scope) |
| **Virtual Threads** | Supported (Java 21+) for blocking operations |

## Key Components

| Class | Responsibility |
| :--- | :--- |
| `Carade` | The application entry point. Initializes Netty and Global State. |
| `WriteSequencer` | The **Single Point of Truth** for mutations. Serializes all writes. |
| `ClientHandler` | Handles individual client sessions, buffers, and command dispatch. |
| `WorkerGroup` | (Netty) Manages the thread pool for handling network events. |

## Extension & Usage

*   **Implementing a New Write Command**:
    *   **DO NOT** use `synchronized` on the command object itself.
    *   Always wrap your mutation logic in a lambda and pass it to `WriteSequencer.getInstance().executeWrite(...)`.
    *   Example:
        ```java
        WriteSequencer.getInstance().executeWrite(() -> {
            db.put(key, value);
        }, rawCommandBytes);
        ```

*   **Read Commands**:
    *   Read operations (e.g., `GET`) do **not** go through the Sequencer. They access the Concurrent data structures directly (fast-path), relying on the thread-safety of `ConcurrentHashMap`.

*   **Deadlock Prevention**:
    *   Since there is a global lock, **never** perform blocking I/O (like network calls to 3rd parties or heavy disk reads) inside the `executeWrite` lambda. This will stall the entire server.
