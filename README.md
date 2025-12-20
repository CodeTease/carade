# 🥕 Carade

> "Storing your bits, forgetting your bytes... but now remembering them better!"

**Carade** is a high-performance, in-memory key-value store & message broker built with pure Java. Now runs on Virtual Threads, supports **RESP (Redis Serialization Protocol)**, extensive Data Structures, and robust Security.

Part of the **CodeTease** Open-Source ecosystem.

## Features

* **Virtual Threads (Java 21+):** Handles millions of concurrent connections effortlessly.
* **RESP Support:** Fully compatible with standard Redis clients (`redis-cli`, `redis-py`, etc.).
* **Advanced Data Structures:**
    * **String:** Standard Key-Value with `INCR`/`DECR` support, `SETBIT`/`GETBIT`.
    * **List:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, and blocking `BLPOP`/`BRPOP`.
    * **Hash:** `HSET`, `HGET`, `HGETALL`, `HDEL`, `HINCRBY`.
    * **Set:** `SADD`, `SMEMBERS`, `SREM`, `SINTER`, `SUNION`, `SDIFF`.
    * **Sorted Set:** `ZADD`, `ZRANGE`, `ZREM`, `ZRANK`, `ZSCORE`...
* **Persistence (AOF):** Append-Only File logging ensures data survives restarts. Replays commands on startup.
* **Pub/Sub System:** 
    * `SUBSCRIBE` / `PUBLISH`.
    * **Pattern Matching:** `PSUBSCRIBE news.*`.
    * `UNSUBSCRIBE` support.
* **Memory Management:** 
    * **Max Memory Limit:** Configurable RAM usage.
    * **LRU Eviction:** Automatically removes least recently used keys when full.
* **Security & Config:**
    * **ACLs:** User-based permissions (Admin, Read/Write, Read-Only) via `AUTH`.
    * **Configuration File:** Load settings from `carade.conf`.

## Roadmap & Missing Features

While Carade aims for high compatibility, the following features are currently **missing** in v0.2.0 compared to standard Redis:

* **Lua Scripting:** No support for `EVAL` or `EVALSHA`.
* **Redis Cluster:** No native clustering or sharding support.
* **Streams:** No support for Stream data type (`XADD`, `XREAD`, etc.).
* **Modules:** No module system support.
* **Advanced Bit Operations:** Missing `BITOP`, `BITCOUNT`, `BITFIELD`.
* **HyperLogLog:** Missing `PFADD`, `PFCOUNT`.
* **ACL Command:** Full `ACL` command management is not implemented (use `carade.conf` for user management).

## Installation & Usage

**Prerequisites:** JDK 21+.

1. **Build**
```bash
javac core/*.java
```

2. **Run**
```bash
# Run with default config or carade.conf if present
java -cp . core.Carade
```

The server listens on port **63790** (default).

## Docker Support 🐳

You can also run Carade using Docker:

```bash
# Pull version 0.2.0 from GHCR
docker pull ghcr.io/codetease/carade:0.2.0

# Run container (Persist data to ./data directory)
docker run -d \
  -p 63790:63790 \
  -v $(pwd)/data:/data \
  --name carade-server \
  ghcr.io/codetease/carade:0.2.0
```

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
* `INCR key` / `DECR key`
* `TTL key` / `EXPIRE key seconds`
* `KEYS pattern`
* `LPUSH key value` / `RPOP key`
* `BLPOP key timeout` / `BRPOP key timeout`
* `HSET key field value` / `HGETALL key`
* `SADD key member` / `SMEMBERS key`
* `ZADD key score member` / `ZRANGE key 0 -1`

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
