# Carade Documentation

Welcome to the official documentation for **Carade**, a high-performance, in-memory key-value store built with Java and Netty.

## Table of Contents

### [Getting Started](getting-started.md)
*   Installation
*   Running via Docker
*   First Steps

### [Architecture](architecture/overview.md)
*   [Overview](architecture/overview.md) - System design and threading model.
*   [Network Layer](architecture/network-layer.md) - Netty and RESP implementation.
*   [Storage Engine](architecture/storage-engine.md) - Data structures and memory management.
*   [Persistence](architecture/persistence.md) - RDB and AOF mechanisms.
*   [Replication](architecture/replication.md) - Master-Replica synchronization.

### [Operations](operations/configuration.md)
*   [Configuration](operations/configuration.md) - `carade.conf` parameters.
*   [Deployment](operations/deployment.md) - Production deployment guide.
*   [Performance Tuning](operations/performance.md) - JVM and OS optimization.

### [Commands](commands/compatibility.md)
*   [Compatibility Matrix](commands/compatibility.md) - Supported Redis commands.
*   [Strings](commands/strings.md)
*   [Lists](commands/lists.md)
*   [Hashes](commands/hashes.md)
*   [Sets](commands/sets.md)
*   [Sorted Sets](commands/sorted-sets.md)
*   [Geospatial](commands/geo.md)
*   [JSON](commands/json.md)
*   [Probabilistic](commands/probabilistic.md)

### [Development](development/contributing.md)
*   [Contributing](development/contributing.md) - How to build and contribute.
*   [Testing](development/testing.md) - Running unit tests and chaos suite.
