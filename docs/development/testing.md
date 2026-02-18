# Testing Carade

Carade uses a comprehensive testing strategy involving unit tests, chaos testing, and benchmarking.

## Unit Tests

Carade uses **JUnit 5** for unit testing. The test suite covers all commands, data structures, and core logic.

### Running Unit Tests

To run all unit tests:

```bash
mvn test
```

To run a specific test class:

```bash
mvn -Dtest=StringCommandTest test
```

## Chaos Engineering

Carade includes a Python-based chaos engineering suite in `tools/chaos/` to test system resilience under failure conditions.

### Prerequisites

*   Python 3.12+
*   Dependencies: `pip install -r tools/chaos/requirements.txt` (if exists) or standard Python libs.

### Running Chaos Tests

The chaos suite runs against a live Carade server.

1.  **Start Carade Server:**
    ```bash
    java -jar target/carade-0.3.4.jar &
    ```

2.  **Run Chaos Script:**
    ```bash
    python3 tools/chaos/run_all.py
    ```

The chaos suite simulates:
*   Network partitions/latency.
*   Random command injection.
*   Transaction conflicts.
*   Protocol fuzzing.

## Benchmarking

Carade includes a high-performance Rust benchmark client in `tools/rust-benchmarks/`.

### Prerequisites

*   **Rust (Cargo)** installed.

### Running Benchmarks

1.  **Build Benchmark Client:**
    ```bash
    cd tools/rust-benchmarks
    cargo build --release
    ```

2.  **Run Benchmark:**
    ```bash
    ./target/release/benchmark --host 127.0.0.1 --port 63790 --clients 50 --requests 100000
    ```

This tool measures latency (p50, p99), throughput (ops/sec), and connection handling.
