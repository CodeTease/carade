# Tools & Scripts Module

The `tools/` directory contains a collection of scripts and auxiliary applications designed to assist developers in testing, benchmarking, and interacting with the Carade server.

## Core Mechanics

### Python Utilities
The root `tools/` folder contains Python scripts for quick verification:
*   **`chat.py`**: A fully functional Pub/Sub chat client. It demonstrates how to parse RESP protocol in Python without using a library (`redis-py`). It handles two concurrent connections: one for publishing messages and one for subscribing to the channel.
*   **`ping.py`**: A minimal script to check server liveness. It sends a raw `PING` command and expects `PONG`.

### Rust Benchmarks
Located in `tools/rust-benchmarks/`, this is a high-performance benchmarking suite written in Rust. It is used to stress-test the server with high concurrency and various payload sizes. (See its own `README.md` for details).

## Technical Specifications

*   **Python Scripts:** Written for Python 3. No external dependencies (uses `socket` and `sys`).
*   **Protocol:** Both `chat.py` and `ping.py` implement a custom, lightweight RESP parser (`read_resp`) to interpret server responses.

## Key Components

| File | Purpose |
| :--- | :--- |
| `tools/chat.py` | Demo CLI Chat application showing `PUBLISH`/`SUBSCRIBE` usage. |
| `tools/ping.py` | Connectivity checker. |
| `tools/rust-benchmarks/` | Advanced load testing tool (Rust). |

## Extension & Usage

### Running the Python Scripts
Ensure your Carade server is running on port `63790` (default).

**1. Ping Check**
```bash
python3 tools/ping.py
```
*Expected Output:* `✅ PONG`

**2. Chat Application**
```bash
python3 tools/chat.py
```
*   Enter your username.
*   Type messages to send to the `general` channel.
*   Run multiple instances in different terminals to chat between them.

**3. Benchmarking**
Refer to `tools/rust-benchmarks/README.md` for instructions on building and running the Rust stress tester.
