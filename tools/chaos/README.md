# Carade Chaos Engineering Tools

This directory contains a suite of Python scripts designed to stress-test and "sabotage" the Carade server (Redis-compatible) to verify its resilience, error handling, and stability.

## Prerequisites

*   Python 3.x
*   Carade Server running on `127.0.0.1:63790` (default) with password `teasertopsecret`.
*   Environment variable `CARADE_PASSWORD` can be set to override the default password.

## 1. Protocol Fuzzing

Tests the `NettyRespDecoder` and `RespParser` against malformed or hostile RESP packets.

```bash
python3 tools/chaos/protocol_fuzz.py
```

*   **Length Dishonesty:** Sends massive length prefixes (`*2147483647`) to test buffer allocation limits.
*   **Recursive Depth:** Sends deeply nested arrays (`[[[[...]]]]`) to trigger StackOverflows.
*   **Partial Frames:** Sends commands byte-by-byte (Slowloris style) to test timeout/thread-pool management.

## 2. Concurrency Stress

Tests the atomicity and isolation of transactions and locking mechanisms.

```bash
python3 tools/chaos/concurrency_stress.py
```

*   **Thundering Herd:** Multiple clients race to `WATCH` a key and perform `MULTI/EXEC` transactions.
*   **Race against Eviction:** Rapidly accesses a key that is constantly expiring (1ms TTL).
*   **Interleaved Transactions:** Verifies that commands from Client B are not blocked or mixed into Client A's transaction before `EXEC`.

## 3. Storage Sabotage

**WARNING: Stop the Carade server before running these tests!**

These scripts modify the persistence files directly to simulate disk corruption or write failures.

```bash
# Truncate the AOF file (simulating power loss during write)
python3 tools/chaos/storage_sabotage.py --action aof-trunc --dir /path/to/data

# Corrupt the RDB file (simulating bit rot)
python3 tools/chaos/storage_sabotage.py --action rdb-flip --dir /path/to/data

# Simulate Disk Full (creates a dummy file)
python3 tools/chaos/storage_sabotage.py --action disk-full --dir /path/to/data
```

After running these, restart Carade and check the logs to ensure it either recovers gracefully or rejects the corrupted file without crashing.

## 4. Resource & Network Stress

Tests resource limits and network handling.

```bash
python3 tools/chaos/resource_net.py [optional_test_name]
```

*   **Pub/Sub Backpressure:** Fast publisher vs. slow subscriber. Checks if server buffers expand indefinitely.
*   **Lua CPU Exhaustion:** Sends `while true do end` script. Checks if server kills the script or hangs.
*   **Rapid Reconnects:** churns connection establishment to stress the file descriptor table.
*   **Zombie Connections:** Opens many idle connections to test resource limits.
