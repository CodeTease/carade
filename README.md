# 🥕 Carade

> "Storing your bits, forgetting your bytes"

**Carade** is a high-performance, in-memory key-value store built with pure Java. It is designed to be lightweight, persistent, and unreasonably fast for a weekend project.

Part of the **CodeTease** Open-Source ecosystem.

## Features

* **Blazingly Fast:** Handles **30k+ RPS** on a potato laptop (Verified, trust me).
* **In-Memory Architecture:** Uses Java's `ConcurrentHashMap` for O(1) access time.
* **Persistence (AOF-ish):** Auto-saves snapshots to disk so you don't lose your memes when the server crashes.
* **Security:** Simple `AUTH` command because we care about your privacy (mostly).
* **Protocol:** Text-based protocol (similar to Redis but simpler).

## Installation & Usage

1. **Build**
You need JDK 11+ (Java 21+ recommended).
```bash
# Get the Carade.java file from "core" folder
javac Carade.java
```

2. **Run**
```bash
java Carade
```

You will see the magnificent ASCII banner. The server listens on port **63790**.

3. **Connect**
You can use `telnet`, `netcat`, or our Python scripts [benchmark.py](benchmark.py).
```
telnet localhost 63790
```

## Commands

| Command           | Description                               | Example                    |
|-------------------|-------------------------------------------|----------------------------|
| `AUTH <password>` | Authenticate (Default: `teasertopsecret`) | `AUTH teasertopsecret`     |
| `SET <key> <val>` | Store a key-value pair                    | `SET user:1 "Teaserverse"` |
| `GET <key>`       | Retrieve a value                          | `GET user:1`               |
| `DEL <key>`       | Delete a key                              | `DEL user:1`               |
| `DBSIZE`          | Show total keys                           | `DBSIZE`                   |
| `SAVE`            | Force save snapshot to disk               | `SAVE`                     |

> Actually, don't use markdown table.

## Ran on local environment using `benchmark.py` (Python Client):
```
Target: Localhost | Mode: Blocking I/O
--------------------------------------
✅ Processed: 50,000 requests
✅ Time:      1.37s
✅ True RPS:  ~36,302 req/s
```

## License

Distributed under the **MIT License**. See [LICENSE](LICENSE).
