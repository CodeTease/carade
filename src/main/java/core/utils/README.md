# Utils Module

The `core.utils` module provides shared utilities and helper classes used throughout the codebase. These utilities ensure consistency in logging, time handling, and common algorithms.

## Core Mechanics

### Deterministic Time
The `Time` class is a critical abstraction for time-dependent operations (like expiration).
*   **Problem:** Using `System.currentTimeMillis()` directly makes it hard to test expiration logic (e.g., "Wait 1 hour").
*   **Solution:** `Time.now()` delegates to an internal `Clock` interface. In production, this uses the system clock. In tests, it can be swapped for a mock clock to "time travel."

### Standardized Logging
The `Log` class wraps `java.util.logging` (JUL) to provide a unified logging interface and format.
*   **Format:** `[LEVEL] Message` (e.g., `[INFO] Server started on port 63790`).
*   **Levels:** `info`, `warn`, `error`, `debug`.
*   **Configuration:** Removes default parent handlers to enforce the custom formatter.

## Technical Specifications

*   **Logging:** Uses `java.util.logging`. No external dependencies like Log4j or SLF4J are required, keeping the artifact size small.
*   **Time:** AtomicReference allows thread-safe clock swapping during runtime (mostly for tests).

## Key Components

| Class | Responsibility |
| :--- | :--- |
| `Time` | Provides `now()` and `setClock(Clock)` for time management. |
| `Log` | Static logging methods (`info`, `warn`, `error`) with a custom console formatter. |
| `GeoUtils` | Helper methods for geospatial calculations (Haversine formula, geohash encoding). |

## Extension & Usage

*   **Using Time:**
    Always use `core.utils.Time.now()` instead of `System.currentTimeMillis()` when dealing with data expiration or timestamps that might need testing.

    ```java
    long now = Time.now();
    if (entry.expireAt < now) { ... }
    ```

*   **Logging:**
    Use `Log` static methods instead of `System.out.println`.
    ```java
    Log.info("New connection from " + address);
    Log.error("Failed to save data: " + e.getMessage());
    ```
