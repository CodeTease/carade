# 🥕 Carade

> "Storing your bits, forgetting your bytes... but now remembering them better!"

**Carade** is a high-performance, in-memory key-value store & message broker built with pure Java. Now runs on Virtual Threads, supports **RESP (Redis Serialization Protocol)**, extensive Data Structures, and robust Security.

Part of the **CodeTease** Open-Source ecosystem.

## Features

* **Virtual Threads (Java 21+):** Handles millions of concurrent connections effortlessly.
* **RESP Support:** Fully compatible with standard Redis clients (`redis-cli`, `redis-py`, etc.).
* **Advanced Data Structures:**
    * **String:** Standard Key-Value.
    * **List:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`.
    * **Hash:** `HSET`, `HGET`, `HGETALL`, `HDEL`.
    * **Set:** `SADD`, `SMEMBERS`, `SREM`.
* **Persistence (AOF):** Append-Only File logging ensures data survives restarts. Replays commands on startup.
* **Pub/Sub System:** 
    * `SUBSCRIBE` / `PUBLISH`.
    * **Pattern Matching:** `PSUBSCRIBE news.*`.
    * `UNSUBSCRIBE` support.
* **Memory Management:** 
    * **Max Memory Limit:** Configurable RAM usage.
    * **LRU Eviction:** Automatically removes least recently used keys when full.
* **Security & Config:**
    * **ACLs:** User-based permissions (Admin, Read/Write, Read-Only).
    * **Configuration File:** Load settings from `carade.conf`.

## Installation & Usage

**Prerequisites:** JDK 21+.

1. **Build**
```bash
javac core/*.java
```

2. **Run**
```bash
# Run with default config or carade.conf if present
java -cp core Carade
```

The server listens on port **63790** (default).

## Configuration (`carade.conf`)

Create a `carade.conf` file to configure the server:

```properties
port 63790
requirepass teasertopsecret
maxmemory 100MB

# Users: user <name> <password> <admin|readwrite|readonly>
user default teasertopsecret admin
user viewer viewpass readonly
user writer writepass readwrite
```

## Usage & Commands

Connect via `redis-cli -p 63790` or `telnet`.

**Storage Commands**
* `SET key value [EX seconds]`
* `GET key`
* `DEL key`
* `LPUSH key value` / `RPOP key`
* `HSET key field value` / `HGETALL key`
* `SADD key member` / `SMEMBERS key`

**Pub/Sub**
* `SUBSCRIBE channel`
* `PSUBSCRIBE pattern.*`
* `PUBLISH channel message`

**System**
* `AUTH [user] password`
* `FLUSHALL`
* `DBSIZE`

## Included Tools

* `benchmark.py`: Stress tester. (Legacy script)
* `chat.py`: Demo chat app.
* `r.py`: Simple Redis REPL for testing purpose. (Ensure installed redis-py using `pip install redis`).

## License

Distributed under the **MIT License**.
