# Rust Benchmark Tools

This module provides a high-performance benchmarking client written in Rust, designed to stress-test the Carade server more efficiently than legacy Python or Java scripts.

## Core Mechanics

### 1. High-Performance Design
*   **Async/Await**: Built on **Tokio**, allowing thousands of concurrent client simulations on a single thread.
*   **Connection Pooling**: Uses the `redis-rs` crate with multiplexed connections to maximize throughput.
*   **Latency Tracking**: Integrates `HdrHistogram` to capture microsecond-level latency distribution (p50, p99, p99.9) without the "Coordinated Omission" problem common in simpler tools.

### 2. Scenarios
The tool supports various pre-defined scenarios to target specific subsystems:
*   **Basic**: Simple SET/GET operations.
*   **Complex**: Operations on Lists, Sets, and Hashes.
*   **LuaStress**: Simulates long-running Lua scripts.
*   **WorkloadSkew**: Generates Zipfian distribution traffic (hot keys).
*   **Backpressure**: Tests server behavior under saturation.
*   **LargePayload**: Tests network bandwidth limits.
*   **Pipeline**: Tests command pipelining throughput.
*   **ConnectionChurn**: Tests rapid connection/disconnection handling.
*   **PubSub**: Tests Publish/Subscribe message throughput.
*   **Probabilistic**: Tests HLL, BloomFilter, and T-Digest operations.

## Technical Specifications

| Requirement | Details |
| :--- | :--- |
| **Language** | Rust |
| **Runtime** | Tokio |
| **Protocol** | Redis (RESP) |
| **Default Port** | 63790 |

## Getting Started

### 1. Prerequisites
If you are a Java developer and don't have Rust installed:

```bash
# Install Rust (Linux/macOS)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Refresh shell
source $HOME/.cargo/env

# Verify installation
cargo --version
```

### 2. Building the Tool
Navigate to this directory and build in release mode (crucial for accurate benchmarks):

```bash
cd tools/rust-benchmarks
cargo build --release
```

The binary will be located at `target/release/carade-benchmark`.

## Usage

### Running a Benchmark
You can run the benchmark directly via `cargo run`:

```bash
# Basic usage (defaults: localhost:63790, 50 clients, 1000 reqs/client)
cargo run --release

# Custom scenario with more load
cargo run --release -- --host 127.0.0.1 --port 63790 --clients 100 --requests 10000 --scenario LuaStress
```

### Available Flags
| Flag | Default | Description |
| :--- | :--- | :--- |
| `--host` | 127.0.0.1 | Target server IP. |
| `--port` | 63790 | Target server port. |
| `--clients` | 50 | Number of concurrent virtual clients. |
| `--requests` | 1000 | Number of requests *per client*. |
| `--scenario` | Basic | Test scenario (Basic, Complex, LuaStress, WorkloadSkew, etc.). |
| `--password` | teasertopsecret | Authentication password. |

## Extension & Integration

*   **Adding a Scenario**:
    1.  Create a new module in `src/scenarios/`.
    2.  Implement an async function `run(client, ...)`
    3.  Register the scenario in the `Scenario` enum in `main.rs`.
    4.  Add the dispatch logic in the main loop.
