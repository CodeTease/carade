# Carade

**Carade** is a high-performance, in-memory key-value store and message broker built purely in Java using Netty. It is designed to be protocol-compatible with Redis, supporting the RESP (Redis Serialization Protocol) to work seamlessly with existing Redis clients.

## Key Features

*   **100% Java:** Runs on any JVM (Java 21+ required).
*   **Netty-Based:** Asynchronous, event-driven network application framework.
*   **RESP Protocol:** Compatible with `redis-cli`, `redis-py`, Jedis, Lettuce, etc.
*   **Rich Data Structures:** Strings, Lists, Sets, Sorted Sets, Hashes, Geospatial, HyperLogLog, Bloom Filters, and JSON.
*   **Persistence:** RDB Snapshots and Append-Only File (AOF) for durability.
*   **Replication:** Master-Replica architecture for high availability.
*   **Lua Scripting:** Atomic execution of server-side scripts.

## Navigation

*   [Getting Started](getting-started.md): Installation and quick start guide.
*   [Architecture](architecture/overview.md): Learn how Carade works under the hood.
*   [Configuration](operations/configuration.md): Detailed configuration reference.
*   [Commands](commands/compatibility.md): List of supported commands.
*   [Development](development/contributing.md): Guide for contributors.
