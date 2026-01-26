package core.replication;

import core.Carade;
import core.network.ClientHandler;
import core.protocol.Resp;
import core.server.WriteSequencer;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplicationManager {
    private static final ReplicationManager INSTANCE = new ReplicationManager();
    public static ReplicationManager getInstance() { return INSTANCE; }

    // --- MASTER ROLE ---
    private final CopyOnWriteArrayList<ClientHandler> replicas = new CopyOnWriteArrayList<>();

    public void addReplica(ClientHandler client) {
        replicas.add(client);
    }

    public void removeReplica(ClientHandler client) {
        replicas.remove(client);
    }

    public void propagate(byte[] commandBytes) {
        if (replicas.isEmpty()) return;
        // Broadcast to all replicas
        for (ClientHandler replica : replicas) {
            replica.sendResponse(commandBytes, null);
        }
    }

    // --- SLAVE ROLE ---
    private volatile String masterHost = null;
    private volatile int masterPort = -1;
    private final AtomicBoolean isSlave = new AtomicBoolean(false);
    private Thread replicationThread;
    
    public boolean isSlave() { return isSlave.get(); }
    public String getMasterHost() { return masterHost; }
    public int getMasterPort() { return masterPort; }
    
    // Simplistic offset (needs proper implementation)
    public long getReplicationOffset() { 
        return WriteSequencer.getInstance().getBacklog().getGlobalOffset();
    }
    
    public int getConnectedReplicasCount() {
        return replicas.size();
    }

    public void slaveOf(String host, int port) {
        if (host == null) {
            // STOP REPLICATION
            isSlave.set(false);
            if (replicationThread != null) {
                replicationThread.interrupt();
            }
            masterHost = null;
            masterPort = -1;
            System.out.println("🔓 Replication: Master mode (SLAVEOF NO ONE)");
        } else {
            // START REPLICATION
            masterHost = host;
            masterPort = port;
            isSlave.set(true);
            startReplicationThread();
            System.out.println("🔗 Replication: Slave mode (SLAVEOF " + host + " " + port + ")");
        }
    }

    private void startReplicationThread() {
        if (replicationThread != null && replicationThread.isAlive()) {
            replicationThread.interrupt();
        }
        replicationThread = new Thread(this::runReplicationLoop, "ReplicationLoop");
        replicationThread.start();
    }

    private void runReplicationLoop() {
        while (isSlave.get()) {
            try (Socket socket = new Socket(masterHost, masterPort)) {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();

                // 1. Handshake
                // PING
                out.write(Resp.array(Arrays.asList("PING".getBytes(StandardCharsets.UTF_8))));
                out.flush();
                Resp.parse(in); // Expect PONG

                // REPLCONF listening-port <port>
                out.write(Resp.array(Arrays.asList(
                    "REPLCONF".getBytes(StandardCharsets.UTF_8), 
                    "listening-port".getBytes(StandardCharsets.UTF_8), 
                    String.valueOf(Carade.config.port).getBytes(StandardCharsets.UTF_8)
                )));
                out.flush();
                Resp.parse(in); // Expect OK

                // PSYNC ? -1
                out.write(Resp.array(Arrays.asList(
                    "PSYNC".getBytes(StandardCharsets.UTF_8), 
                    "?".getBytes(StandardCharsets.UTF_8), 
                    "-1".getBytes(StandardCharsets.UTF_8)
                )));
                out.flush();
                
                // Read Response (expecting +FULLRESYNC or +CONTINUE)
                Resp.parse(in);
                
                // Read RDB Snapshot
                // Expecting a Bulk String containing the RDB file
                Resp.Request rdbReq = Resp.parse(in);
                if (rdbReq != null && !rdbReq.args.isEmpty()) {
                    byte[] rdbData = rdbReq.args.get(0);
                    System.out.println("📥 Replication: Received RDB Snapshot (" + rdbData.length + " bytes). Loading...");
                    
                    Carade.db.clearAll();
                    try {
                        new core.persistence.rdb.RdbParser(new ByteArrayInputStream(rdbData)).parse(Carade.db);
                        System.out.println("✅ Replication: RDB Loaded. Keys: " + Carade.db.size());
                    } catch (Exception e) {
                        System.err.println("❌ Replication: RDB Load Failed: " + e.getMessage());
                    }
                }

                // 2. Read Command Stream
                while (isSlave.get() && !Thread.currentThread().isInterrupted()) {
                    Resp.Request req = Resp.parse(in);
                    if (req == null) break;
                    
                    final List<byte[]> parts = req.args;
                    if (parts.isEmpty()) continue;
                    
                    // Reconstruct raw bytes for logging/propagation
                    byte[] rawCmd = Resp.array(parts);
                    
                    // Execute locally using WriteSequencer to ensure AOF/Backlog consistency
                    WriteSequencer.getInstance().executeWrite(() -> {
                         // Calls the internal command execution logic
                         Carade.executeAofCommand(parts);
                    }, rawCmd);
                }

            } catch (Exception e) {
                if (isSlave.get()) {
                    System.err.println("⚠️ Replication Sync Error: " + e.getMessage() + ". Retrying in 3s...");
                    try { Thread.sleep(3000); } catch (InterruptedException ex) { break; }
                }
            }
        }
    }
}
