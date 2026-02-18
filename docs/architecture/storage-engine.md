# Storage Engine

Carade is an **in-memory data store**. All data is stored in the Java Heap (JVM memory).

## Data Structure

The core data structure is a `ConcurrentHashMap` of `String` keys to `ValueEntry` objects.

```java
public class ValueEntry {
    private Object value;
    private DataType type;
    public long expireAt = -1; // -1 = no expiry
    public long lastAccess;
    public int accessCount;
}
```

### Supported Data Types (Internal Representation)

| Redis Type | Java Implementation | Details |
| :--- | :--- | :--- |
| **String** | `byte[]` | Raw byte array for max compatibility. |
| **List** | `ConcurrentLinkedDeque<String>` | Thread-safe, non-blocking doubly-linked list. |
| **Set** | `ConcurrentHashMap.KeySetView` | Set backed by ConcurrentHashMap. |
| **Hash** | `ConcurrentHashMap<String, String>` | Thread-safe key-value map. |
| **ZSet** | `CaradeZSet` | Custom SkipList implementation (`ConcurrentSkipListMap` + `ConcurrentHashMap`). |
| **Geo** | `GeoHash` | Encoded as `ZSet` (Geohash -> Member). |
| **JSON** | `JsonNode` (Jackson) | Parsed JSON tree. |
| **TDigest** | `TDigest` | Centroid-based quantile estimation. |
| **Bloom** | `BloomFilter` | Murmur3-based probabilistic filter. |

## Database Partitions

Carade partitions data into **16 logical databases** (indices 0-15), similar to Redis.
Each database is an independent `ConcurrentHashMap`. Clients can switch databases using the `SELECT` command.

## Expiration Mechanism

Carade uses a **Lazy + Active** expiration strategy:

1.  **Lazy Expiration:**
    *   Whenever a key is accessed (e.g., via `GET`), Carade checks if the key has expired (`expireAt < System.currentTimeMillis()`).
    *   If expired, the key is deleted immediately, and `null` is returned.

2.  **Active Expiration (Janitor):**
    *   A background thread runs periodically (default 100ms).
    *   It samples random keys from the database.
    *   If a key is expired, it is removed.
    *   This prevents memory leaks from keys that are set with a TTL but never accessed again.

## Memory Management

Carade relies on the JVM Garbage Collector (G1GC or ZGC recommended) to reclaim memory from deleted objects.

### Eviction Policies (maxmemory)
When memory usage exceeds the configured `maxmemory`, Carade attempts to free space based on the policy:

*   **noeviction:** Returns error on write commands (default).
*   **allkeys-lru:** Evicts least recently used keys.
*   **volatile-lru:** Evicts least recently used keys with an expire set.
*   **allkeys-random:** Evicts random keys.
*   **volatile-random:** Evicts random keys with an expire set.
