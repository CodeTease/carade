# Persistence

Carade supports two data persistence modes: **RDB (Snapshotting)** and **AOF (Append Only File)**.

## RDB (Redis Database File)

Carade periodically saves the entire dataset to disk (`carade.dump`). This creates a point-in-time snapshot of your dataset.

### Mechanism
*   **Trigger:**
    *   **Manual:** `SAVE` or `BGSAVE`.
    *   **Automatic:** Configurable (e.g., `save 900 1` -> save after 900 seconds if 1 key changed).
*   **Format:**
    *   Binary format, optimized for loading speed.
    *   Compatible with Redis RDB structure (version 6/7/9).
*   **Implementation:**
    *   Carade uses `RdbEncoder` to iterate over all databases and serialize keys/values.
    *   Unlike Redis (which forks a process), Carade uses a **background thread** to save data. This avoids copy-on-write overhead but consumes heap memory during save.

## AOF (Append Only File)

Carade logs every write operation received by the server to an append-only file (`carade.aof`).

### Mechanism
*   **Logging:**
    *   Every write command (e.g., `SET key value`) is appended to the AOF immediately after execution.
    *   Commands are stored in the RESP format.
*   **Consistency:**
    *   Commands are logged *after* successful execution in memory.
    *   This ensures only valid commands are persisted.
*   **Replay:**
    *   On server startup, Carade reads the AOF file from beginning to end to reconstruct the dataset.
    *   This provides higher durability than RDB.

### AOF Rewrite (`BGREWRITEAOF`)
Over time, the AOF grows indefinitely. Carade supports **AOF Rewrite**:
1.  Creates a new AOF based on the current in-memory dataset.
2.  Writes the minimal set of commands needed to rebuild the current state.
3.  Example: Instead of `INCR k` (100 times), it writes `SET k 100`.

## Configuration
Persistence settings can be adjusted in `carade.conf`.

```properties
# Snapshotting
save 900 1
save 300 10
save 60 10000

# Append Only File
appendonly yes
appendfilename "carade.aof"
```
