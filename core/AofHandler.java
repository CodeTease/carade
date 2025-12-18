import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class AofHandler {
    private final String filename;
    private PrintWriter writer;
    private final ScheduledExecutorService flusher;
    // Buffer for commands received during rewrite
    private final ConcurrentLinkedQueue<String> rewriteBuffer = new ConcurrentLinkedQueue<>();
    private volatile boolean isRewriting = false;
    
    public AofHandler(String filename) {
        this.filename = filename;
        try {
            // Append mode, autoFlush = false (buffered)
            // Use BufferedWriter explicitly for better buffering control, though PrintWriter wraps it usually.
            this.writer = new PrintWriter(new BufferedWriter(new FileWriter(filename, true))); 
        } catch (IOException e) {
            System.err.println("⚠️ Could not open AOF file: " + e.getMessage());
        }

        // Background flusher (fsync every 1s)
        this.flusher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "AOF-Flusher");
            t.setDaemon(true);
            return t;
        });
        this.flusher.scheduleAtFixedRate(this::flush, 1, 1, TimeUnit.SECONDS);
    }

    public synchronized void log(String cmd, String... args) {
        List<String> parts = new ArrayList<>();
        parts.add(cmd);
        parts.addAll(Arrays.asList(args));
        String resp = Resp.array(parts);
        
        if (writer != null) {
            writer.print(resp);
        }
        
        if (isRewriting) {
            rewriteBuffer.add(resp);
        }
    }

    private synchronized void flush() {
        if (writer != null) {
            writer.flush();
        }
    }

    public void rewrite(Map<String, Carade.ValueEntry> store) {
        isRewriting = true;
        rewriteBuffer.clear();
        
        String tempFile = filename + ".tmp";
        // Use a flag to track if we should try to rename. 
        // We cannot rely on try-with-resources to close BEFORE we rename inside the block.
        // So we must structure it differently.
        PrintWriter tempWriter = null;
        try {
            tempWriter = new PrintWriter(new BufferedWriter(new FileWriter(tempFile)));
            for (Map.Entry<String, Carade.ValueEntry> entry : store.entrySet()) {
                String key = entry.getKey();
                Carade.ValueEntry val = entry.getValue();
                if (val.isExpired()) continue;

                // Reconstruct commands based on type
                if (val.type == Carade.DataType.STRING) {
                     String v = new String((byte[]) val.value, java.nio.charset.StandardCharsets.UTF_8);
                     // Expiry logic? If it has expiry, we should preserve it.
                     // Current Carade.ValueEntry stores expireAt (absolute time).
                     // SET key value EX ttl
                     if (val.expireAt > 0) {
                         long ttl = (val.expireAt - System.currentTimeMillis()) / 1000;
                         if (ttl <= 0) continue; // Expired
                         tempWriter.print(Resp.array(Arrays.asList("SET", key, v, "EX", String.valueOf(ttl))));
                     } else {
                         tempWriter.print(Resp.array(Arrays.asList("SET", key, v)));
                     }
                } else if (val.type == Carade.DataType.LIST) {
                    ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) val.value;
                    // RPUSH key v1 v2 ... (But our RPUSH takes one arg currently, so multiple RPUSHes)
                    // Or better: Redis RPUSH accepts multiple args. Carade only 1.
                    // So we must emit multiple RPUSH or modify Carade to accept multiple.
                    // Carade `RPUSH key value` in `executeInternal` only takes 1 value.
                    // So we must loop.
                    for (String s : list) {
                        tempWriter.print(Resp.array(Arrays.asList("RPUSH", key, s)));
                    }
                } else if (val.type == Carade.DataType.HASH) {
                    ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) val.value;
                    for (Map.Entry<String, String> e : map.entrySet()) {
                        tempWriter.print(Resp.array(Arrays.asList("HSET", key, e.getKey(), e.getValue())));
                    }
                } else if (val.type == Carade.DataType.SET) {
                    Set<String> set = (Set<String>) val.value;
                    for (String s : set) {
                        tempWriter.print(Resp.array(Arrays.asList("SADD", key, s)));
                    }
                } else if (val.type == Carade.DataType.ZSET) {
                    Carade.CaradeZSet zset = (Carade.CaradeZSet) val.value;
                    // ZADD key score member
                    // We can batch, but for simplicity one by one is fine for rewrite
                    for (Map.Entry<String, Double> e : zset.scores.entrySet()) {
                        tempWriter.print(Resp.array(Arrays.asList("ZADD", key, String.valueOf(e.getValue()), e.getKey())));
                    }
                }
            }
            
            // Append commands that happened during rewrite
            // We need to lock briefly to ensure we get the last commands and stop buffering
            synchronized(this) {
                for (String cmd : rewriteBuffer) {
                    tempWriter.print(cmd);
                }
                tempWriter.flush();
                isRewriting = false;
                rewriteBuffer.clear();
                
                // Close the temp writer explicitly BEFORE rename
                tempWriter.close();
                tempWriter = null;

                // Now swap files inside the lock to prevent new logs from being lost/split badly
                // Close current writer to release lock/handle
                if (writer != null) {
                    writer.flush();
                    writer.close();
                }

                File temp = new File(tempFile);
                File dest = new File(filename);
                if (dest.exists()) dest.delete();
                
                if (temp.renameTo(dest)) {
                     System.out.println("✅ AOF Rewrite successful.");
                } else {
                     System.err.println("⚠️ AOF Rewrite failed to rename temp file.");
                }
                
                // Re-open
                try {
                   this.writer = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
                } catch (IOException e) {
                    System.err.println("⚠️ FATAL: Could not re-open AOF: " + e.getMessage());
                }
            }
            
        } catch (IOException e) {
            System.err.println("⚠️ AOF Rewrite failed: " + e.getMessage());
            isRewriting = false;
        } finally {
            if (tempWriter != null) tempWriter.close();
        }
    }
    
    public void replay(java.util.function.Consumer<List<String>> commandExecutor) {
        File f = new File(filename);
        if (!f.exists()) return;
        
        System.out.println("📂 Replaying AOF...");
        try (FileInputStream fis = new FileInputStream(f)) {
            while (true) {
                Resp.Request req = Resp.parse(fis);
                if (req == null) break;
                
                if (!req.args.isEmpty()) {
                    commandExecutor.accept(req.args);
                }
            }
        } catch (IOException e) {
            System.err.println("⚠️ Error replaying AOF: " + e.getMessage());
        }
    }
    
    public void close() {
        flusher.shutdown();
        try {
            if (!flusher.awaitTermination(2, TimeUnit.SECONDS)) {
                flusher.shutdownNow();
            }
        } catch (InterruptedException e) {
            flusher.shutdownNow();
        }
        if (writer != null) {
            writer.flush();
            writer.close();
        }
    }
}
