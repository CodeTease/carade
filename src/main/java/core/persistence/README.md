# Persistence Module

This module handles the RDB (Redis Database) persistence mechanism, ensuring data durability by snapshotting the in-memory dataset to disk.

## Save Flow

### SAVE Command
The `SAVE` command invokes `Carade.saveData()` directly on the main thread.
*   **Blocking**: The server stops processing other commands until the save is complete.
*   **Use case**: Shutdown or explicit user request when blocking is acceptable.

### BGSAVE Command
The `BGSAVE` command invokes `Carade.saveData()` in a separate thread.
*   **Non-Blocking**: The main thread continues to serve clients.
*   **Mechanism**: Unlike Redis which uses `fork()` for copy-on-write, this implementation spawns a Java `Thread`.
    *   *Note*: Since Java threads share heap memory, this is not a true point-in-time snapshot if writes occur during the save process, unless the underlying data structures support concurrent iteration with snapshot isolation (e.g., `ConcurrentHashMap` iterators are weakly consistent).

## RDB File Structure

The `core.persistence.rdb.RdbEncoder` class generates RDB files following the version 9 standard.

### Structure
1.  **Header**: `REDIS0009` (Magic + Version).
2.  **Auxiliary Fields**: Metadata like `redis-ver`.
3.  **Database Selector**: `SELECTDB` opcode followed by the DB index.
4.  **Key-Value Pairs**:
    *   **Expire**: `EXPIRETIMEMS` opcode + timestamp (if applicable).
    *   **Type**: Byte indicating type (String, List, Set, ZSet, Hash).
    *   **Key**: Encoded string.
    *   **Value**: Type-specific encoding.
5.  **EOF**: `0xFF` opcode.
6.  **Checksum**: 8 bytes (currently set to 0/disabled).

### Encoding Details
*   **Length Encoding**: Uses Redis standard 6-bit, 14-bit, and 32-bit length prefixes.
*   **Compression**: Strings larger than 20 bytes are compressed using **LZ4** if it saves space.
