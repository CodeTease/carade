# Database Module

The `core.db` module is responsible for the in-memory storage, organization, and lifecycle management of data within Carade. It implements the key-value store mechanics, including expiration, eviction policies, and data structure wrapping.

## Core Mechanics

### Data Organization
Carade mimics the Redis model of multiple logical databases.
*   **Databases:** The `CaradeDatabase` singleton holds an array of `ConcurrentHashMap<String, ValueEntry>`, defaulting to 16 databases (indexed 0-15).
*   **Thread Safety:** The underlying maps are thread-safe, but complex operations (like `RENAME` or `FLUSHALL`) rely on global locks managed at the Command execution level.

### The `ValueEntry` Wrapper
Every value stored in the database is wrapped in a `ValueEntry` object. This wrapper holds metadata essential for memory management:
*   **Value:** The actual data (String, List, Set, etc.).
*   **DataType:** Enum identifying the type (e.g., `STRING`, `HASH`, `ZSET`).
*   **Metadata:**
    *   `expireAt`: Timestamp for expiration (-1 if persistent).
    *   `lastAccessed`: Timestamp for LRU (Least Recently Used) eviction.
    *   `frequency`: Counter for LFU (Least Frequently Used) eviction.

### Eviction & Expiration
*   **Expiration:** Implements a "Lazy + Active" strategy.
    *   *Lazy:* Checks if a key is expired when accessed via `get()`.
    *   *Active:* A background "Janitor" task periodically samples keys to remove expired ones.
*   **Eviction:** Triggered when memory usage exceeds `maxmemory`. The `performEvictionIfNeeded()` method samples keys and removes them based on the configured policy (`allkeys-lru`, `volatile-random`, etc.).

## Technical Specifications

*   **Storage Engine:** `java.util.concurrent.ConcurrentHashMap`.
*   **Max Memory:** Configurable limit. When reached, write operations trigger eviction cycles.
*   **Notifications:** Supports Keyspace Notifications (Pub/Sub) for events like `expired`, `del`, `set`.

## Key Components

| Class | Responsibility |
| :--- | :--- |
| `CaradeDatabase` | The singleton manager for all data. Handles `get`, `put`, `remove`, and eviction logic. |
| `ValueEntry` | Wrapper class for stored values, handling metadata (TTL, LRU info) and serialization helpers. |
| `DataType` | Enumeration of supported data types (`STRING`, `LIST`, `HASH`, etc.). |

## Extension & Usage

*   **Adding New Data Types:**
    1.  Add a new entry to `DataType` enum.
    2.  Implement the logic in a new command class.
    3.  If the type requires complex serialization/copying, update `ValueEntry.copy()` and `ValueEntry.compress()`.

*   **Integration:**
    *   Commands interact with this module via `CaradeDatabase.getInstance()`.
    *   Persistence layers (RDB/AOF) read directly from the `CaradeDatabase.databases` array to snapshot data.
