# Deployment

Carade can be deployed as a standard Java application or using Docker.

## 1. Docker Deployment (Recommended)

The easiest way to run Carade in production is using Docker.

### Running with Data Persistence

To ensure data persists across container restarts, mount a volume for `/data`.

```bash
docker run -d \
  -p 63790:63790 \
  -v /opt/carade/data:/data \
  --name carade-server \
  ghcr.io/codetease/carade:latest
```

### Custom Configuration

To use a custom `carade.conf`, mount it into the container:

```bash
docker run -d \
  -p 63790:63790 \
  -v /opt/carade/carade.conf:/app/carade.conf \
  -v /opt/carade/data:/data \
  ghcr.io/codetease/carade:latest
```

## 2. Bare Metal (JAR) Deployment

For maximum performance, run Carade directly on the host machine.

### Requirements

*   **Java 21 JDK** or later.
*   **Maven** (for building).

### Build

```bash
mvn clean package -DskipTests
```

### Run as a Systemd Service

Create a service file at `/etc/systemd/system/carade.service`:

```ini
[Unit]
Description=Carade In-Memory Store
After=network.target

[Service]
User=carade
Group=carade
ExecStart=/usr/bin/java -Xms4G -Xmx4G -XX:+UseZGC -jar /opt/carade/carade-0.3.4.jar
WorkingDirectory=/opt/carade
Restart=always

[Install]
WantedBy=multi-user.target
```

Reload and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable carade
sudo systemctl start carade
```

## Security Best Practices

1.  **Network Isolation:** Bind to `127.0.0.1` if only accessed locally, or use a firewall (iptables/UFW) to restrict access to port 63790.
2.  **Authentication:** Always set a strong `requirepass` in `carade.conf`.
3.  **Renaming Commands:** Use `rename-command` in config (if supported) or disable dangerous commands like `FLUSHALL` via ACLs.
