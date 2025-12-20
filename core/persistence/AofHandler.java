package core.persistence;

import core.db.CaradeDatabase;
import core.db.ValueEntry;
import core.db.DataType;
import core.structs.CaradeZSet;
import core.protocol.Resp;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.nio.charset.StandardCharsets;

public class AofHandler {
    private final String filename;
    private OutputStream outStream;
    private final ScheduledExecutorService flusher;
    // Buffer for commands received during rewrite
    private final ConcurrentLinkedQueue<byte[]> rewriteBuffer = new ConcurrentLinkedQueue<>();
    private volatile boolean isRewriting = false;
    
    private static AofHandler INSTANCE;

    public static synchronized AofHandler getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new AofHandler("carade.aof");
        }
        return INSTANCE;
    }

    private AofHandler(String filename) {
        this.filename = filename;
        INSTANCE = this; 
        try {
            this.outStream = new BufferedOutputStream(new FileOutputStream(filename, true));
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
    
    // For testing or manual init
    public AofHandler(File file) {
        this(file.getAbsolutePath());
    }

    public synchronized void log(String cmd, Object... args) {
        List<byte[]> parts = new ArrayList<>();
        parts.add(cmd.getBytes(StandardCharsets.UTF_8));
        for (Object arg : args) {
            if (arg instanceof String) {
                parts.add(((String) arg).getBytes(StandardCharsets.UTF_8));
            } else if (arg instanceof byte[]) {
                parts.add((byte[]) arg);
            } else {
                parts.add(String.valueOf(arg).getBytes(StandardCharsets.UTF_8));
            }
        }
        byte[] resp = Resp.array(parts);
        
        if (outStream != null && resp != null) {
            try {
                outStream.write(resp);
            } catch (IOException e) {
                System.err.println("⚠️ Failed to write to AOF: " + e.getMessage());
            }
        }
        
        if (isRewriting) {
            rewriteBuffer.add(resp);
        }
    }

    private synchronized void flush() {
        if (outStream != null) {
            try {
                outStream.flush();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    public void rewrite(CaradeDatabase db) {
        isRewriting = true;
        rewriteBuffer.clear();
        
        String tempFile = filename + ".tmp";
        OutputStream tempOut = null;
        try {
            tempOut = new BufferedOutputStream(new FileOutputStream(tempFile));
            
            for (int i=0; i<CaradeDatabase.DB_COUNT; i++) {
                if (db.size(i) == 0) continue;
                
                // Switch to DB i
                writeCommand(tempOut, "SELECT", String.valueOf(i).getBytes(StandardCharsets.UTF_8));
                
                for (Map.Entry<String, ValueEntry> entry : db.entrySet(i)) {
                    String key = entry.getKey();
                    ValueEntry val = entry.getValue();
                    if (val.isExpired()) continue;

                    // Reconstruct commands based on type
                    if (val.type == DataType.STRING) {
                         byte[] v = (byte[]) val.value;
                         if (val.expireAt > 0) {
                             long ttl = (val.expireAt - System.currentTimeMillis()) / 1000;
                             if (ttl <= 0) continue; // Expired
                             writeCommand(tempOut, "SET", key.getBytes(StandardCharsets.UTF_8), v, "EX".getBytes(StandardCharsets.UTF_8), String.valueOf(ttl).getBytes(StandardCharsets.UTF_8));
                         } else {
                             writeCommand(tempOut, "SET", key.getBytes(StandardCharsets.UTF_8), v);
                         }
                    } else if (val.type == DataType.LIST) {
                        ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) val.value;
                        for (String s : list) {
                            writeCommand(tempOut, "RPUSH", key.getBytes(StandardCharsets.UTF_8), s.getBytes(StandardCharsets.UTF_8));
                        }
                    } else if (val.type == DataType.HASH) {
                        ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) val.value;
                        for (Map.Entry<String, String> e : map.entrySet()) {
                            writeCommand(tempOut, "HSET", key.getBytes(StandardCharsets.UTF_8), e.getKey().getBytes(StandardCharsets.UTF_8), e.getValue().getBytes(StandardCharsets.UTF_8));
                        }
                    } else if (val.type == DataType.SET) {
                        Set<String> set = (Set<String>) val.value;
                        for (String s : set) {
                            writeCommand(tempOut, "SADD", key.getBytes(StandardCharsets.UTF_8), s.getBytes(StandardCharsets.UTF_8));
                        }
                    } else if (val.type == DataType.ZSET) {
                        CaradeZSet zset = (CaradeZSet) val.value;
                        for (Map.Entry<String, Double> e : zset.scores.entrySet()) {
                             writeCommand(tempOut, "ZADD", key.getBytes(StandardCharsets.UTF_8), String.valueOf(e.getValue()).getBytes(StandardCharsets.UTF_8), e.getKey().getBytes(StandardCharsets.UTF_8));
                        }
                    }
                }
            }
            
            // Append commands that happened during rewrite
            synchronized(this) {
                for (byte[] cmd : rewriteBuffer) {
                    tempOut.write(cmd);
                }
                tempOut.flush();
                isRewriting = false;
                rewriteBuffer.clear();
                
                tempOut.close();
                tempOut = null;

                if (outStream != null) {
                    outStream.flush();
                    outStream.close();
                }

                File temp = new File(tempFile);
                File dest = new File(filename);
                if (dest.exists()) dest.delete();
                
                if (temp.renameTo(dest)) {
                     System.out.println("✅ AOF Rewrite successful.");
                } else {
                     System.err.println("⚠️ AOF Rewrite failed to rename temp file.");
                }
                
                try {
                   this.outStream = new BufferedOutputStream(new FileOutputStream(filename, true));
                } catch (IOException e) {
                    System.err.println("⚠️ FATAL: Could not re-open AOF: " + e.getMessage());
                }
            }
            
        } catch (IOException e) {
            System.err.println("⚠️ AOF Rewrite failed: " + e.getMessage());
            isRewriting = false;
        } finally {
            if (tempOut != null) try { tempOut.close(); } catch (IOException e) {}
        }
    }
    
    private void writeCommand(OutputStream out, String cmd, byte[]... args) throws IOException {
        List<byte[]> parts = new ArrayList<>();
        parts.add(cmd.getBytes(StandardCharsets.UTF_8));
        for (byte[] arg : args) parts.add(arg);
        out.write(Resp.array(parts));
    }
    
    public void replay(java.util.function.Consumer<List<byte[]>> commandExecutor) {
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
        if (outStream != null) {
            try {
                outStream.flush();
                outStream.close();
            } catch (IOException e) { }
        }
    }
}
