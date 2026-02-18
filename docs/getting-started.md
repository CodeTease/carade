# Getting Started with Carade

This guide will help you install and run Carade in less than 5 minutes.

## Prerequisites

Carade requires **Java 21** or later.

```bash
java -version
# Should output something like: openjdk version "21.0.1" ...
```

## Installation

### Method 1: Running from Source (Recommended for Dev)

Clone the repository and build with Maven:

```bash
git clone https://github.com/codetease/carade.git
cd carade
mvn clean package
```

Run the server:

```bash
java -jar target/carade-0.3.4.jar
```

The server will start on port **63790** by default.

### Method 2: Docker

You can run Carade using the official Docker image:

```bash
docker pull ghcr.io/codetease/carade:latest

# Run with persistence mapped to local ./data directory
docker run -d \
  -p 63790:63790 \
  -v $(pwd)/data:/data \
  --name carade-server \
  ghcr.io/codetease/carade:latest
```

## Connecting to Carade

Since Carade uses the standard RESP protocol, you can use any Redis client.

**Using redis-cli:**

```bash
# Connect to default port 63790
redis-cli -p 63790

# Authenticate (default password: teasertopsecret)
127.0.0.1:63790> AUTH teasertopsecret
OK

# Test command
127.0.0.1:63790> SET foo bar
OK
127.0.0.1:63790> GET foo
"bar"
```

## Next Steps

*   [Configuration](operations/configuration.md) - Learn how to configure ports, memory limits, and users.
*   [Architecture](architecture/overview.md) - Understand the threading model and storage engine.
*   [Commands](commands/compatibility.md) - See supported commands.
