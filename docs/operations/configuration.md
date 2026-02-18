# Configuration

Carade is configured using a `carade.conf` file (or `carade.yaml`). By default, the server looks for `carade.conf` in the working directory.

## Sample Configuration

```properties
# Network
port 63790
bind 0.0.0.0

# Security
requirepass teasertopsecret
# User ACL: user <name> <password> <role>
user default teasertopsecret admin
user viewer viewpass readonly
user writer writepass readwrite

# Memory
maxmemory 256MB
maxmemory-policy noeviction

# Persistence
appendonly yes
appendfilename "carade.aof"
save 900 1
save 300 10
```

## Parameters

### Network

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `port` | `63790` | The TCP port the server listens on. |
| `bind` | `0.0.0.0` | Network interface to bind to (0.0.0.0 = all). |

### Security

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `requirepass` | `teasertopsecret` | Password required for connection. Applies to `default` user. |
| `user <name> <pass> <role>` | - | Define ACL users. Roles: `admin`, `readwrite`, `readonly`. |

### Memory Management

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `maxmemory` | `256MB` | Maximum memory limit (e.g., `1GB`, `500MB`). Set to `0` for unlimited. |
| `maxmemory-policy` | `noeviction` | Eviction policy when limit is reached. |

**Supported Policies:**
*   `noeviction`: Return error on write.
*   `allkeys-lru`: Remove LRU keys.
*   `volatile-lru`: Remove LRU keys with expire set.
*   `allkeys-random`: Remove random keys.
*   `volatile-random`: Remove random keys with expire set.

### Persistence

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `appendonly` | `yes` | Enable Append Only File (AOF) persistence. |
| `appendfilename` | `carade.aof` | Name of the AOF file. |
| `save <seconds> <changes>` | - | Save RDB snapshot if `changes` keys changed in `seconds`. |

## Environment Variables

Carade also respects the following environment variables, which override the config file:

*   `CARADE_PASSWORD`: Overrides `requirepass`.
*   `CARADE_VERSION`: Overrides server version string.
