# Performance Tuning

Carade is a Java-based application, so performance tuning is heavily dependent on the Java Virtual Machine (JVM) configuration.

## 1. JVM Memory Settings

The most critical setting is the Heap Size (`-Xms` and `-Xmx`).

*   **Heap Size:** Should be larger than `maxmemory` in `carade.conf`.
*   **Recommendation:** Set `Xms` (Initial Heap) equal to `Xmx` (Max Heap) to avoid heap resizing overhead.
*   **Overhead:** Allow ~20-30% overhead for JVM internals (metadata, threads, GC structures).

Example for a 4GB instance:
```bash
# carade.conf: maxmemory 3GB
java -Xms4G -Xmx4G -jar carade.jar
```

## 2. Garbage Collection (GC)

For low latency, use **ZGC** (available in Java 21) or **Shenandoah**.

### ZGC (Recommended for Latency)
ZGC is designed for sub-millisecond pause times regardless of heap size.

```bash
-XX:+UseZGC -XX:+ZGenerational
```

### G1GC (Alternative)
If ZGC is not available or desired, G1GC is a solid choice for throughput/latency balance.

```bash
-XX:+UseG1GC -XX:MaxGCPauseMillis=20
```

## 3. OS Tuning

### Linux Kernel Settings

Add the following to `/etc/sysctl.conf`:

```bash
# Allow more open file descriptors (connections)
fs.file-max = 100000

# TCP Keepalive settings (Netty relies on this)
net.ipv4.tcp_keepalive_time = 300
net.ipv4.tcp_keepalive_intvl = 60
net.ipv4.tcp_keepalive_probes = 3
```

Apply with `sysctl -p`.

### Transparent Huge Pages (THP)

Disable THP to prevent latency spikes during memory allocation.

```bash
echo never > /sys/kernel/mm/transparent_hugepage/enabled
echo never > /sys/kernel/mm/transparent_hugepage/defrag
```

## 4. Netty Configuration

Carade uses Netty for I/O. By default, it uses `Runtime.getRuntime().availableProcessors() * 2` worker threads.

To customize (if supported via system properties):
```bash
-Dio.netty.eventLoopThreads=8
```
