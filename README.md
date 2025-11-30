# 🥕 Carade

> "Storing your bits, forgetting your bytes"

**Carade** is a high-performance, in-memory key-value store & message broker built with pure Java. Now runs on Virtual Threads, supports Pub/Sub, and auto-migrates its own data.

Part of the **CodeTease** Open-Source ecosystem.

## Features

* **Virtual Threads (Java 21+):** We ditched the thread pool for `Executors.newVirtualThreadPerTaskExecutor()`. It can theoretically handle millions of connections (or until your RAM cries).
* **Pub/Sub System:** Turn your database into a chat server. `SUBSCRIBE` to channels and `PUBLISH` gossip instantly.
* **Time-To-Live (TTL):** Data doesn't have to be eternal. Use `SET key val EX 60` to let it die peacefully.
* **Auto-Migration:** The server automatically detects old data formats (String) and upgrades them to the new format (ValueEntry) on startup. Magic!
* **Real-time Dashboard:** The console now prints live stats (OPS, Clients, Keys) every 5 seconds. Ideally suited for looking busy at work.
* **Smart Security:** Passwords are loaded from `CARADE_PASSWORD` env var (or default to `teasertopsecret` because we know you're lazy).

## Installation & Usage

**Prerequisites:** You need **JDK 21** or higher. We need those Virtual Threads.

1. **Build**
```bash
javac core/Carade.java
```

2. **Run**
```bash
# Run from the root directory
java -cp core Carade
```

You will see the magnificent ASCII banner. The server listens on port **63790**.

## Usage & Commands
Connect via `telnet localhost 63790` or use our included Python tools.

**Storage Commands**

* `SET <k> <v> [EX <s>]`
    * * **Desc:** Store a key (optional TTL in seconds).
    * * **Ex:** `SET status "Online" EX 60`

* `GET <key>`
    * **Desc:** Retrieve a value.
    * **Ex:** `GET status`

* `DEL <key>`
    * **Desc:** Delete a key.
    * **Ex:** `DEL status`

**Pub/Sub Commands**

* `SUBSCRIBE <channel>`
    * **Desc:** Listen for messages (Blocks client until QUIT).
    * **Ex:** `SUBSCRIBE general`

* `PUBLISH <chan> <msg>`
    * **Desc:** Send message to all subscribers in a channel.
    * **Ex:** `PUBLISH general "Hello!"`

**System Commands**

* `AUTH <password>`
    * **Desc:** Authenticate with the server.
    * **Ex:** `AUTH teasertopsecret`

* `DBSIZE`
    * **Desc:** Show total number of keys.

* `FLUSHALL`
    * **Desc:** Nuke EVERYTHING. (Use with caution).

## Included Tools

We included some python scripts because typing manual TCP commands is painful.

1. **Stress Tester (`benchmark.py`)**

* Multi-threaded stress test using Python.
* Verifies features (TTL, Quotes) automatically.
* Usage: `python benchmark.py`

2. **Chat Demo (chat.py)**

* A proof-of-concept chat app using Carade's Pub/Sub.
* Open two terminals and talk to yourself!
* Usage: `python chat.py`

# Configuration

* `CARADE_PASSWORD`
    * **Default:** `teasertopsecret`
    * **Description:** The password required to `AUTH`. Set this env var in production (if you ever dare to run this in production).

## License

Distributed under the **MIT License**. See [LICENSE](LICENSE).
