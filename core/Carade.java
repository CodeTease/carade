import java.lang.management.ManagementFactory;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Project: Carade
 * Version: 0.1.0 (The "Gossip" Universe Edition)
 * Status: Alpha / Dev / Chaos / Forever
 * Author: CodeTease Team
 * "We don't bump versions, we bump features."
 */
public class Carade {

    // --- CONFIGURATION ---
    private static final String DUMP_FILE = "carade.dump";
    private static final String AOF_FILE = "carade.aof";
    private static Config config;
    
    // --- METRICS ---
    private static final AtomicLong totalCommands = new AtomicLong(0);
    private static final AtomicInteger activeConnections = new AtomicInteger(0);

    // --- STORAGE ENGINE ---
    enum DataType { STRING, LIST, HASH, SET, ZSET }

    static class ZNode implements Comparable<ZNode> {
        double score;
        String member;
        ZNode(double score, String member) { this.score = score; this.member = member; }
        @Override public int compareTo(ZNode o) {
            int c = Double.compare(this.score, o.score);
            return c != 0 ? c : this.member.compareTo(o.member);
        }
        @Override public boolean equals(Object o) {
            if (o instanceof ZNode) {
                ZNode z = (ZNode)o;
                return score == z.score && member.equals(z.member);
            }
            return false;
        }
        @Override public int hashCode() { return Objects.hash(score, member); }
    }

    static class CaradeZSet {
         final ConcurrentHashMap<String, Double> scores = new ConcurrentHashMap<>();
         final ConcurrentSkipListSet<ZNode> sorted = new ConcurrentSkipListSet<>();
         
         int add(double score, String member) {
             Double oldScore = scores.get(member);
             if (oldScore != null) {
                 if (oldScore == score) return 0;
                 sorted.remove(new ZNode(oldScore, member));
                 scores.put(member, score);
                 sorted.add(new ZNode(score, member));
                 return 0; // Updated
             }
             scores.put(member, score);
             sorted.add(new ZNode(score, member));
             return 1; // New
         }
         
         Double score(String member) { return scores.get(member); }
         
         int size() { return scores.size(); }
    }

    static class ValueEntry implements Serializable {
        private static final long serialVersionUID = 2L;
        Object value;
        DataType type;
        long expireAt; 

        long lastAccessed;

        ValueEntry(Object value, DataType type, long ttlSeconds) {
            this.value = value;
            this.type = type;
            this.expireAt = ttlSeconds > 0 ? System.currentTimeMillis() + (ttlSeconds * 1000) : -1;
            this.lastAccessed = System.nanoTime();
        }
        
        // Constructor for legacy migration (String only)
        ValueEntry(String value, long ttlSeconds) {
            this(value, DataType.STRING, ttlSeconds);
        }

        boolean isExpired() { return expireAt != -1 && System.currentTimeMillis() > expireAt; }
        
        void touch() { this.lastAccessed = System.nanoTime(); }
    }

    private static ConcurrentHashMap<String, ValueEntry> store = new ConcurrentHashMap<>();
    private static AofHandler aofHandler;

    // --- BLOCKING QUEUES ---
    static class BlockingRequest {
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        final boolean isLeft;
        BlockingRequest(boolean isLeft) { this.isLeft = isLeft; }
    }
    private static final ConcurrentHashMap<String, ConcurrentLinkedQueue<BlockingRequest>> blockingRegistry = new ConcurrentHashMap<>();

    private static void checkBlockers(String key) {
        ConcurrentLinkedQueue<BlockingRequest> q = blockingRegistry.get(key);
        if (q == null) return;
        
        synchronized (q) {
            while (!q.isEmpty()) {
                ValueEntry v = store.get(key);
                if (v == null || v.type != DataType.LIST) return;
                ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.value;
                if (list.isEmpty()) return;
                
                BlockingRequest req = q.peek();
                if (req.future.isDone()) {
                    q.poll(); 
                    continue;
                }
                
                String val = req.isLeft ? list.pollFirst() : list.pollLast();
                
                if (val != null) {
                    q.poll(); 
                    boolean completed = req.future.complete(Arrays.asList(key, val));
                    if (!completed) {
                        if (req.isLeft) list.addFirst(val); else list.addLast(val); // Revert
                        continue;
                    }
                    if (aofHandler != null) aofHandler.log(req.isLeft ? "LPOP" : "RPOP", key);
                    if (list.isEmpty()) store.remove(key);
                } else {
                    return;
                }
            }
        }
    }

    // --- PUB/SUB ENGINE (NEW!) ---
    private static PubSub pubSub = new PubSub();

    private static volatile boolean isRunning = true;

    public static void printBanner() {
        System.out.println("\n" +
                "   ______                     __   \n" +
                "  / ____/___ ______________  / /__ \n" +
                " / /   / __ `/ ___/ __  / __  / _ \\\n" +
                "/ /___/ /_/ / /  / /_/ / /_/ /  __/\n" +
                "\\____/\\__,_/_/   \\__,_/\\__,_/\\___/ \n" +
                "                                   \n" +
                " :: Carade ::       (v0.1.0-alpha) \n" +
                " :: Engine ::       Java Virtual Threads 🚀\n" +
                " :: Feature ::      Pub/Sub Enabled 📡\n");
    }

    public static void main(String[] args) {
        System.out.println("\n--- CARADE v0.1.0 (The 'Gossip' Universe) ---\n");
        
        // Load Config
        config = Config.load("carade.conf");
        // Override with env var if present (legacy support)
        if (System.getenv("CARADE_PASSWORD") != null) {
            config.password = System.getenv("CARADE_PASSWORD");
            config.users.get("default").password = config.password;
        }

        // Initialize AOF
        aofHandler = new AofHandler(AOF_FILE);

        loadData();
        // Replay AOF over loaded data (AOF takes precedence if we decide so, or we use AOF only if dump missing?)
        // The user prompt suggests "Upgrade: ... replace dump... or AOF".
        // Let's rely on AOF if it exists. If dump exists but AOF is empty, maybe convert?
        // Simple approach: Load dump first (legacy), then replay AOF.
        // But if AOF is comprehensive, we don't need dump.
        // For migration: if we have dump and no AOF, we load dump.
        // Future writes go to AOF.
        aofHandler.replay(cmd -> executeInternal(cmd));

        // 1. Janitor
        Thread janitor = new Thread(() -> {
            while (isRunning) {
                try {
                    Thread.sleep(30000); 
                    cleanupExpiredKeys(); 
                    // saveData(); // Disable Dump saving to rely on AOF? 
                    // Or keep it for snapshotting?
                    // "Dump file incompatible... heavy...". User suggests "Append-Only File... replay log".
                    // Let's keep snapshot as a backup or for faster startup if we implement rewrite.
                    // For now, let's keep it but maybe less frequent? Or just keep it.
                    saveData();
                } catch (InterruptedException e) { break; }
            }
        });
        janitor.setDaemon(true);
        janitor.start();

        // 2. Monitor
        Thread monitor = new Thread(() -> {
            long lastCount = 0;
            while (isRunning) {
                try {
                    Thread.sleep(5000);
                    long currentCount = totalCommands.get();
                    long ops = (currentCount - lastCount) / 5;
                    lastCount = currentCount;
                    
                    if (ops > 0 || activeConnections.get() > 0) {
                        System.out.printf("📊 [STATS] Clients: %d | Keys: %d | Channels: %d | Patterns: %d | OPS: %d cmd/s\n", 
                            activeConnections.get(), store.size(), pubSub.getChannelCount(), pubSub.getPatternCount(), ops);
                    }
                } catch (InterruptedException e) { break; }
            }
        });
        monitor.setDaemon(true);
        monitor.start();

        printBanner();

        // 3. Start Server
        try (ServerSocket server = new ServerSocket(config.port);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            
            System.out.println("🔥 Ready on port " + config.port);
            System.out.println("🔒 Max Memory: " + (config.maxMemory == 0 ? "Unlimited" : config.maxMemory + " bytes"));

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutting down...");
                saveData();
                aofHandler.close();
            }));

            while (isRunning) {
                Socket client = server.accept();
                executor.execute(new ClientHandler(client));
            }
        } catch (IOException e) {
            System.err.println("💥 Server crash: " + e.getMessage());
        }
    }

    // --- LOGIC HELPER ---
    // Internal method to execute commands from AOF replay (or other internal sources)
    private static final AtomicInteger writeCounter = new AtomicInteger(0);

    // Eviction Policy (Approximate LRU)
    private static void performEvictionIfNeeded() {
        if (config.maxMemory <= 0) return;
        
        // Check only every 50 writes to save CPU
        if (writeCounter.incrementAndGet() % 50 != 0) return;
        
        long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (used < config.maxMemory) return;
        
        System.out.println("🧹 [Eviction] Memory full (" + (used/1024/1024) + "MB > " + (config.maxMemory/1024/1024) + "MB). Evicting...");
        
        // Evict loop
        int attempts = 0;
        Iterator<String> it = store.keySet().iterator();
        while (used > config.maxMemory && !store.isEmpty() && attempts < 100) {
            // Pick a few random keys from iterator (cheap sampling)
            // We just iterate forward, which is "random enough" for hash map
            String bestKey = null;
            long oldestTime = Long.MAX_VALUE;
            
            int samples = 0;
            while (it.hasNext() && samples < 5) {
                String key = it.next();
                ValueEntry v = store.get(key);
                if (v != null) {
                    if (v.lastAccessed < oldestTime) {
                        oldestTime = v.lastAccessed;
                        bestKey = key;
                    }
                }
                samples++;
            }
            // If iterator exhausted, restart
            if (!it.hasNext()) it = store.keySet().iterator();
            
            if (bestKey != null) {
                store.remove(bestKey);
                if (aofHandler != null) aofHandler.log("DEL", bestKey);
            }
            
            used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            attempts++;
        }
    }

    // Internal method to execute commands from AOF replay (or other internal sources)
    private static void executeInternal(List<String> parts) {
        if (parts.isEmpty()) return;
        String cmd = parts.get(0).toUpperCase();
        try {
            switch (cmd) {
                case "SET":
                    if (parts.size() >= 3) {
                        long ttl = -1;
                        if (parts.size() >= 5 && parts.get(3).equalsIgnoreCase("EX")) {
                            try { ttl = Long.parseLong(parts.get(4)); } catch (Exception e) {}
                        }
                        store.put(parts.get(1), new ValueEntry(parts.get(2), DataType.STRING, ttl));
                    }
                    break;
                case "DEL":
                    if (parts.size() >= 2) store.remove(parts.get(1));
                    break;
                case "HDEL":
                    if (parts.size() >= 3) {
                        String key = parts.get(1);
                        String field = parts.get(2);
                        store.computeIfPresent(key, (k, v) -> {
                            if (v.type == DataType.HASH) {
                                ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.value;
                                map.remove(field);
                                if (map.isEmpty()) return null;
                            }
                            return v;
                        });
                    }
                    break;
                case "SREM":
                    if (parts.size() >= 3) {
                         String key = parts.get(1);
                         String member = parts.get(2);
                         store.computeIfPresent(key, (k, v) -> {
                            if (v.type == DataType.SET) {
                                Set<String> set = (Set<String>) v.value;
                                set.remove(member);
                                if (set.isEmpty()) return null;
                            }
                            return v;
                        });
                    }
                    break;
                case "ZADD":
                    if (parts.size() >= 4) {
                        String key = parts.get(1);
                        try {
                             store.compute(key, (k, v) -> {
                                 CaradeZSet zset;
                                 if (v == null) {
                                     zset = new CaradeZSet();
                                     v = new ValueEntry(zset, DataType.ZSET, -1);
                                 } else if (v.type == DataType.ZSET) {
                                     zset = (CaradeZSet)v.value;
                                 } else {
                                     return v;
                                 }
                                 
                                 for (int i = 2; i < parts.size(); i += 2) {
                                     try {
                                         double score = Double.parseDouble(parts.get(i));
                                         String member = parts.get(i+1);
                                         zset.add(score, member);
                                     } catch (Exception ex) {}
                                 }
                                 return v;
                             });
                        } catch (Exception e) {}
                    }
                    break;
                case "MSET":
                    if (parts.size() >= 3) {
                        for (int i = 1; i < parts.size(); i += 2) {
                            if (i + 1 < parts.size()) {
                                String key = parts.get(i);
                                String val = parts.get(i+1);
                                store.put(key, new ValueEntry(val, DataType.STRING, -1));
                            }
                        }
                    }
                    break;
                // Add new types to internal executor
                case "LPUSH":
                case "RPUSH":
                    if (parts.size() >= 3) {
                        String key = parts.get(1);
                        String val = parts.get(2);
                        store.compute(key, (k, v) -> {
                            if (v == null) {
                                ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>();
                                list.add(val);
                                return new ValueEntry(list, DataType.LIST, -1);
                            } else if (v.type == DataType.LIST) {
                                ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.value;
                                if (cmd.equals("LPUSH")) list.addFirst(val); else list.addLast(val);
                                return v;
                            }
                            return v;
                        });
                    }
                    break;
                case "LPOP":
                case "RPOP":
                     if (parts.size() >= 2) {
                        String key = parts.get(1);
                        store.computeIfPresent(key, (k, v) -> {
                            if (v.type == DataType.LIST) {
                                ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.value;
                                if (!list.isEmpty()) {
                                    if (cmd.equals("LPOP")) list.pollFirst(); else list.pollLast();
                                }
                                if (list.isEmpty()) return null;
                            }
                            return v;
                        });
                     }
                     break;
                case "HSET":
                    if (parts.size() >= 4) {
                        String key = parts.get(1);
                        String field = parts.get(2);
                        String val = parts.get(3);
                        store.compute(key, (k, v) -> {
                            if (v == null) {
                                ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
                                map.put(field, val);
                                return new ValueEntry(map, DataType.HASH, -1);
                            } else if (v.type == DataType.HASH) {
                                ((ConcurrentHashMap<String, String>) v.value).put(field, val);
                            }
                            return v;
                        });
                    }
                    break;
                case "SADD":
                    if (parts.size() >= 3) {
                         String key = parts.get(1);
                         String member = parts.get(2);
                         store.compute(key, (k, v) -> {
                            if (v == null) {
                                Set<String> set = ConcurrentHashMap.newKeySet();
                                set.add(member);
                                return new ValueEntry(set, DataType.SET, -1);
                            } else if (v.type == DataType.SET) {
                                ((Set<String>) v.value).add(member);
                            }
                            return v;
                        });
                    }
                    break;
                case "FLUSHALL":
                    store.clear();
                    break;
                case "INCR":
                case "DECR":
                    if (parts.size() >= 2) {
                        String key = parts.get(1);
                        store.compute(key, (k, v) -> {
                            long val = 0;
                            if (v == null) {
                                val = 0;
                            } else if (v.type == DataType.STRING) {
                                try {
                                    val = Long.parseLong((String)v.value);
                                } catch (Exception e) { return v; } 
                            } else {
                                return v;
                            }
                            if (cmd.equals("INCR")) val++; else val--;
                            ValueEntry newV = new ValueEntry(String.valueOf(val), DataType.STRING, -1);
                            if (v != null) newV.expireAt = v.expireAt;
                            return newV;
                        });
                    }
                    break;
                case "EXPIRE":
                    if (parts.size() >= 3) {
                         String key = parts.get(1);
                         try {
                             long seconds = Long.parseLong(parts.get(2));
                             store.computeIfPresent(key, (k, v) -> {
                                 v.expireAt = System.currentTimeMillis() + (seconds * 1000);
                                 return v;
                             });
                         } catch (Exception e) {}
                    }
                    break;
                // Add other state-changing commands here as we implement them
            }
        } catch (Exception e) {
            System.err.println("⚠️ Error executing internal command: " + cmd + " - " + e.getMessage());
        }
    }

    private static void cleanupExpiredKeys() {
        int removed = 0;
        Iterator<Map.Entry<String, ValueEntry>> it = store.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ValueEntry> entry = it.next();
            if (entry.getValue().isExpired()) {
                it.remove();
                // Should we log expiration to AOF? 
                // Technically yes, to keep replicas/AOF consistent if we replay with time, 
                // but usually AOF stores explicit DEL or we rely on TTL logic during replay.
                // If we replay, we re-set the TTL.
                // So no need to log explicit DEL for expiration unless we want deterministic replay at a point in time.
                // Redis usually synthesizes a DEL when a key expires in the master to send to replicas/AOF.
                // Let's do that for correctness.
                if (aofHandler != null) aofHandler.log("DEL", entry.getKey());
                removed++;
            }
        }
        if (removed > 0) System.out.println("🧹 [GC] Vacuumed " + removed + " expired keys.");
    }

    // --- SNAPSHOT FORMAT (RDB-ish) ---
    // Magic: "CARD" (4 bytes)
    // Version: 1 (int)
    // Entry:
    //   Type: 1 byte (0=STRING, 1=LIST, 2=HASH, 3=SET)
    //   Expiry: 8 bytes (long)
    //   Key: String (UTF-8, len prefixed)
    //   Value: Depends on type
    
    private static void saveData() {
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(DUMP_FILE)))) {
            dos.writeBytes("CARD");
            dos.writeInt(1); // Version
            
            for (Map.Entry<String, ValueEntry> entry : store.entrySet()) {
                ValueEntry v = entry.getValue();
                if (v.isExpired()) continue;
                
                // Type
                int typeCode = 0;
                if (v.type == DataType.LIST) typeCode = 1;
                else if (v.type == DataType.HASH) typeCode = 2;
                else if (v.type == DataType.SET) typeCode = 3;
                else if (v.type == DataType.ZSET) typeCode = 4;
                dos.writeByte(typeCode);
                
                // Expiry
                dos.writeLong(v.expireAt);
                
                // Key
                writeString(dos, entry.getKey());
                
                // Value
                switch (v.type) {
                    case STRING:
                        writeString(dos, (String) v.value);
                        break;
                    case LIST:
                        ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.value;
                        dos.writeInt(list.size());
                        for (String s : list) writeString(dos, s);
                        break;
                    case HASH:
                        ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.value;
                        dos.writeInt(map.size());
                        for (Map.Entry<String, String> e : map.entrySet()) {
                            writeString(dos, e.getKey());
                            writeString(dos, e.getValue());
                        }
                        break;
                    case SET:
                        Set<String> set = (Set<String>) v.value;
                        dos.writeInt(set.size());
                        for (String s : set) writeString(dos, s);
                        break;
                    case ZSET:
                        CaradeZSet zset = (CaradeZSet) v.value;
                        dos.writeInt(zset.size());
                        for (Map.Entry<String, Double> e : zset.scores.entrySet()) {
                            writeString(dos, e.getKey());
                            dos.writeDouble(e.getValue());
                        }
                        break;
                }
            }
            System.out.println("💾 Snapshot saved.");
        } catch (IOException e) {
            System.err.println("⚠️ Save failed: " + e.getMessage());
        }
    }
    
    private static void writeString(DataOutputStream dos, String s) throws IOException {
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }
    
    private static String readString(DataInputStream dis) throws IOException {
        int len = dis.readInt();
        byte[] bytes = new byte[len];
        dis.readFully(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void loadData() {
        File f = new File(DUMP_FILE);
        if (!f.exists()) return;
        
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(f)))) {
            // Check Magic
            byte[] magic = new byte[4];
            dis.readFully(magic);
            if (!Arrays.equals(magic, "CARD".getBytes())) {
                // Fallback to legacy object stream if magic doesn't match?
                // The legacy format starts with serialization header (AC ED ...).
                // "CARD" is 43 41 52 44.
                // If it fails, we can try legacy load or just fail.
                // But since we are replacing the format, let's try to detect legacy?
                // For simplicity, let's assume if it doesn't match, we try legacy load.
                // But we can't rewind easily without PushbackInputStream or re-opening.
                throw new IOException("Invalid magic header");
            }
            
            int version = dis.readInt();
            if (version != 1) throw new IOException("Unknown version: " + version);
            
            store = new ConcurrentHashMap<>();
            while (dis.available() > 0) {
                try {
                    int typeCode = dis.readByte();
                    long expireAt = dis.readLong();
                    String key = readString(dis);
                    
                    DataType type = DataType.STRING;
                    Object value = null;
                    
                    if (typeCode == 0) {
                        type = DataType.STRING;
                        value = readString(dis);
                    } else if (typeCode == 1) {
                        type = DataType.LIST;
                        int size = dis.readInt();
                        ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>();
                        for (int i=0; i<size; i++) list.add(readString(dis));
                        value = list;
                    } else if (typeCode == 2) {
                        type = DataType.HASH;
                        int size = dis.readInt();
                        ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
                        for (int i=0; i<size; i++) {
                            String k = readString(dis);
                            String v = readString(dis);
                            map.put(k, v);
                        }
                        value = map;
                    } else if (typeCode == 3) {
                        type = DataType.SET;
                        int size = dis.readInt();
                        Set<String> set = ConcurrentHashMap.newKeySet();
                        for (int i=0; i<size; i++) set.add(readString(dis));
                        value = set;
                    } else if (typeCode == 4) {
                        type = DataType.ZSET;
                        int size = dis.readInt();
                        CaradeZSet zset = new CaradeZSet();
                        for (int i=0; i<size; i++) {
                            String member = readString(dis);
                            double score = dis.readDouble();
                            zset.add(score, member);
                        }
                        value = zset;
                    }
                    
                    ValueEntry ve = new ValueEntry(value, type, -1);
                    ve.expireAt = expireAt; // Restore expiry
                    if (!ve.isExpired()) store.put(key, ve);
                    
                } catch (EOFException e) { break; }
            }
            System.out.println("📂 Loaded " + store.size() + " keys (Snapshot).");
            
        } catch (Exception e) {
            // Try legacy load if magic failed?
            // "Legacy" load used ObjectInputStream.
            System.out.println("⚠️ Snapshot load failed (" + e.getMessage() + "). Trying legacy load...");
            loadLegacyData();
        }
    }
    
    private static void loadLegacyData() {
         File f = new File(DUMP_FILE);
         try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f))) {
            Object loaded = ois.readObject();
            if (loaded instanceof ConcurrentHashMap) {
                ConcurrentHashMap<?, ?> rawMap = (ConcurrentHashMap<?, ?>) loaded;
                store = new ConcurrentHashMap<>();
                for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                    String key = (String) entry.getKey();
                    Object val = entry.getValue();
                    if (val instanceof String) {
                        store.put(key, new ValueEntry((String) val, DataType.STRING, -1));
                    } else if (val instanceof ValueEntry) {
                        store.put(key, (ValueEntry) val);
                    }
                }
                System.out.println("📂 Loaded " + store.size() + " keys (Legacy).");
                // Immediately save in new format
                saveData();
            }
        } catch (Exception ex) {
            System.out.println("⚠️ Legacy load failed. Starting fresh.");
            store = new ConcurrentHashMap<>();
        }
    }

    // --- HANDLER ---
    private static class ClientHandler implements Runnable, PubSub.Subscriber {
        private final Socket socket;
        private Config.User currentUser = null; // null = not authenticated
        private boolean isSubscribed = false; 
        private OutputStream outStream; // Keep ref for PubSub callbacks
        private boolean currentIsResp = false; // Track protocol mode for async callbacks

        public ClientHandler(Socket socket) { this.socket = socket; }

        private synchronized void send(OutputStream out, boolean isResp, String respData, String textData) {
            try {
                if (isResp) out.write(respData.getBytes());
                else out.write((textData + "\n").getBytes());
                out.flush();
            } catch (IOException e) {
                // Ignore, connection likely closed
            }
        }

        private boolean isWriteCommand(String cmd) {
            return Arrays.asList("SET", "DEL", "LPUSH", "RPUSH", "LPOP", "RPOP", 
                                 "HSET", "HDEL", "SADD", "SREM", "FLUSHALL").contains(cmd);
        }
        
        private boolean isAdminCommand(String cmd) {
            return Arrays.asList("FLUSHALL", "DBSIZE").contains(cmd);
        }

        @Override
        public void send(String channel, String message, String pattern) {
            // Callback from PubSub engine
            if (currentIsResp) {
                if (pattern != null) {
                    // PMESSAGE pattern channel message
                    List<String> resp = Arrays.asList("pmessage", pattern, channel, message);
                    send(outStream, true, Resp.array(resp), null);
                } else {
                    // MESSAGE channel message
                    List<String> resp = Arrays.asList("message", channel, message);
                    send(outStream, true, Resp.array(resp), null);
                }
            } else {
                // Text mode
                if (pattern != null) send(outStream, false, null, "[MSG][" + pattern + "] " + channel + ": " + message);
                else send(outStream, false, null, "[MSG] " + channel + ": " + message);
            }
        }

        @Override
        public boolean isResp() { return currentIsResp; }
        
        @Override
        public Object getId() { return this; }

        @Override
        public void run() {
            activeConnections.incrementAndGet();
            try (InputStream in = socket.getInputStream();
                 OutputStream out = socket.getOutputStream()) {
                this.outStream = out;
                
                while (true) {
                    Resp.Request req = Resp.parse(in);
                    if (req == null) break;
                    List<String> parts = req.args;
                    this.currentIsResp = req.isResp;
                    boolean isResp = req.isResp;

                    if (parts.isEmpty()) continue;
                    totalCommands.incrementAndGet();

                    String cmd = parts.get(0).toUpperCase();

                    // Handle Subs commands in Sub mode
                    if (isSubscribed) {
                        if (cmd.equals("QUIT")) break;
                        if (cmd.equals("SUBSCRIBE") || cmd.equals("UNSUBSCRIBE") || 
                            cmd.equals("PSUBSCRIBE") || cmd.equals("PUNSUBSCRIBE")) {
                            // Process valid commands while subscribed
                        } else {
                             // Ignore other commands
                             continue;
                        }
                    }

                    if (currentUser == null && !cmd.equals("AUTH") && !cmd.equals("QUIT")) {
                        send(out, isResp, Resp.error("NOAUTH Authentication required"), "(error) NOAUTH Authentication required.");
                        continue;
                    }
                    
                    // Permission Check
                    if (currentUser != null && !currentUser.canWrite && isWriteCommand(cmd)) {
                         send(out, isResp, Resp.error("ERR permission denied"), "(error) ERR permission denied");
                         continue;
                    }
                    if (currentUser != null && !currentUser.isAdmin && isAdminCommand(cmd)) {
                         send(out, isResp, Resp.error("ERR permission denied"), "(error) ERR permission denied");
                         continue;
                    }

                    try {
                        switch (cmd) {
                            case "AUTH":
                                if (parts.size() < 2) send(out, isResp, Resp.error("usage: AUTH [user] password"), "(error) usage: AUTH [user] password");
                                else {
                                    String user = "default";
                                    String pass = "";
                                    if (parts.size() == 2) {
                                        pass = parts.get(1);
                                    } else {
                                        user = parts.get(1);
                                        pass = parts.get(2);
                                    }
                                    
                                    Config.User u = config.users.get(user);
                                    if (u != null && u.password.equals(pass)) {
                                        currentUser = u;
                                        send(out, isResp, Resp.simpleString("OK"), "OK");
                                    } else {
                                        send(out, isResp, Resp.error("WRONGPASS invalid username-password pair"), "(error) WRONGPASS invalid username-password pair");
                                    }
                                }
                                break;

                            case "SET":
                                if (parts.size() < 3) send(out, isResp, Resp.error("usage: SET key value [EX seconds]"), "(error) usage: SET key value [EX seconds]");
                                else {
                                    performEvictionIfNeeded();
                                    long ttl = -1;
                                    String key = parts.get(1);
                                    String val = parts.get(2);
                                    if (parts.size() >= 5 && parts.get(3).equalsIgnoreCase("EX")) {
                                        try { ttl = Long.parseLong(parts.get(4)); } catch (Exception e) {}
                                    }
                                    store.put(key, new ValueEntry(val, DataType.STRING, ttl));
                                    // Log to AOF
                                    if (ttl > 0) aofHandler.log("SET", key, val, "EX", String.valueOf(ttl));
                                    else aofHandler.log("SET", key, val);
                                    
                                    send(out, isResp, Resp.simpleString("OK"), "OK");
                                }
                                break;

                            case "MSET":
                                if (parts.size() < 3 || (parts.size() - 1) % 2 != 0) {
                                    send(out, isResp, Resp.error("wrong number of arguments for 'mset' command"), "(error) wrong number of arguments for 'mset' command");
                                } else {
                                    performEvictionIfNeeded();
                                    for (int i = 1; i < parts.size(); i += 2) {
                                        String key = parts.get(i);
                                        String val = parts.get(i + 1);
                                        store.put(key, new ValueEntry(val, DataType.STRING, -1));
                                    }
                                    // Log MSET command as is
                                    if (aofHandler != null) {
                                        String[] logArgs = new String[parts.size() - 1];
                                        for(int i=1; i<parts.size(); i++) logArgs[i-1] = parts.get(i);
                                        aofHandler.log("MSET", logArgs);
                                    }
                                    send(out, isResp, Resp.simpleString("OK"), "OK");
                                }
                                break;

                            case "GET":
                                if (parts.size() < 2) send(out, isResp, Resp.error("usage: GET key"), "(error) usage: GET key");
                                else {
                                    ValueEntry entry = store.get(parts.get(1));
                                    if (entry == null) send(out, isResp, Resp.bulkString(null), "(nil)");
                                    else if (entry.isExpired()) { store.remove(parts.get(1)); send(out, isResp, Resp.bulkString(null), "(nil)"); }
                                    else { 
                                        entry.touch(); // LRU update
                                        if (entry.type != DataType.STRING && entry.type != null) {
                                            send(out, isResp, Resp.error("WRONGTYPE Operation against a key holding the wrong kind of value"), "(error) WRONGTYPE");
                                        } else {
                                            String v = (String) entry.value;
                                            send(out, isResp, Resp.bulkString(v), v.contains(" ") ? "\"" + v + "\"" : v);
                                        }
                                    }
                                }
                                break;

                            case "MGET":
                                if (parts.size() < 2) {
                                    send(out, isResp, Resp.error("wrong number of arguments for 'mget' command"), "(error) wrong number of arguments for 'mget' command");
                                } else {
                                    List<String> results = new ArrayList<>();
                                    for (int i = 1; i < parts.size(); i++) {
                                        String key = parts.get(i);
                                        ValueEntry entry = store.get(key);
                                        if (entry == null) {
                                            results.add(null);
                                        } else if (entry.isExpired()) {
                                            store.remove(key);
                                            results.add(null);
                                        } else if (entry.type != DataType.STRING) {
                                            results.add(null);
                                        } else {
                                            entry.touch();
                                            results.add((String) entry.value);
                                        }
                                    }
                                    if (isResp) {
                                        send(out, true, Resp.array(results), null);
                                    } else {
                                        StringBuilder sb = new StringBuilder();
                                        for (int i = 0; i < results.size(); i++) {
                                            String val = results.get(i);
                                            sb.append((i+1) + ") " + (val == null ? "(nil)" : "\"" + val + "\"") + "\n");
                                        }
                                        send(out, false, null, sb.toString().trim());
                                    }
                                }
                                break;

                            case "TTL":
                                if (parts.size() < 2) send(out, isResp, Resp.error("usage: TTL key"), "(error) usage: TTL key");
                                else {
                                    String key = parts.get(1);
                                    ValueEntry entry = store.get(key);
                                    if (entry == null) send(out, isResp, Resp.integer(-2), "(integer) -2");
                                    else if (entry.expireAt == -1) send(out, isResp, Resp.integer(-1), "(integer) -1");
                                    else {
                                        long ttl = (entry.expireAt - System.currentTimeMillis()) / 1000;
                                        if (ttl < 0) {
                                            store.remove(key);
                                            send(out, isResp, Resp.integer(-2), "(integer) -2");
                                        } else {
                                            send(out, isResp, Resp.integer(ttl), "(integer) " + ttl);
                                        }
                                    }
                                }
                                break;

                            case "EXPIRE":
                                if (parts.size() < 3) send(out, isResp, Resp.error("usage: EXPIRE key seconds"), "(error) usage: EXPIRE key seconds");
                                else {
                                    String key = parts.get(1);
                                    try {
                                        long seconds = Long.parseLong(parts.get(2));
                                        final int[] ret = {0};
                                        store.computeIfPresent(key, (k, v) -> {
                                            v.expireAt = System.currentTimeMillis() + (seconds * 1000);
                                            ret[0] = 1;
                                            return v;
                                        });
                                        if (ret[0] == 1) aofHandler.log("EXPIRE", key, String.valueOf(seconds));
                                        send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                                    } catch (NumberFormatException e) {
                                        send(out, isResp, Resp.error("ERR value is not an integer or out of range"), "(error) ERR value is not an integer or out of range");
                                    }
                                }
                                break;

                            case "KEYS":
                                if (parts.size() < 2) send(out, isResp, Resp.error("usage: KEYS pattern"), "(error) usage: KEYS pattern");
                                else {
                                    String pattern = parts.get(1);
                                    List<String> keys = new ArrayList<>();
                                    if (pattern.equals("*")) {
                                        keys.addAll(store.keySet());
                                    } else {
                                        String regex = pattern.replace(".", "\\.").replace("*", ".*").replace("?", ".");
                                        java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex);
                                        for (String k : store.keySet()) {
                                            if (p.matcher(k).matches()) keys.add(k);
                                        }
                                    }
                                    if (isResp) send(out, true, Resp.array(keys), null);
                                    else {
                                        StringBuilder sb = new StringBuilder();
                                        for (int i = 0; i < keys.size(); i++) sb.append((i+1) + ") \"" + keys.get(i) + "\"\n");
                                        send(out, false, null, sb.toString().trim());
                                    }
                                }
                                break;

                            case "INCR":
                            case "DECR":
                                if (parts.size() < 2) send(out, isResp, Resp.error("usage: "+cmd+" key"), "(error) usage: "+cmd+" key");
                                else {
                                    performEvictionIfNeeded();
                                    String key = parts.get(1);
                                    final long[] ret = {0};
                                    try {
                                        store.compute(key, (k, v) -> {
                                            long val = 0;
                                            if (v == null) {
                                                val = 0;
                                            } else if (v.type != DataType.STRING) {
                                                throw new RuntimeException("WRONGTYPE Operation against a key holding the wrong kind of value");
                                            } else {
                                                try {
                                                    val = Long.parseLong((String)v.value);
                                                } catch (NumberFormatException e) {
                                                    throw new RuntimeException("ERR value is not an integer or out of range");
                                                }
                                            }
                                            
                                            if (cmd.equals("INCR")) val++; else val--;
                                            ret[0] = val;
                                            
                                            ValueEntry newV = new ValueEntry(String.valueOf(val), DataType.STRING, -1);
                                            if (v != null) newV.expireAt = v.expireAt;
                                            newV.touch();
                                            return newV;
                                        });
                                        aofHandler.log(cmd, key);
                                        send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                                    } catch (RuntimeException e) {
                                        String msg = e.getMessage();
                                        if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                                            send(out, isResp, Resp.error(msg), "(error) " + msg);
                                        else throw e;
                                    }
                                }
                                break;
                            
                            // --- LISTS ---
                            case "LPUSH":
                            case "RPUSH":
                                if (parts.size() < 3) send(out, isResp, Resp.error("usage: "+cmd+" key value"), "(error) usage: "+cmd+" key value");
                                else {
                                    performEvictionIfNeeded();
                                    String key = parts.get(1);
                                    String val = parts.get(2); // In real Redis, multiple values allowed
                                    try {
                                        store.compute(key, (k, v) -> {
                                            if (v == null) {
                                            ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>();
                                                list.add(val);
                                                return new ValueEntry(list, DataType.LIST, -1);
                                            } else if (v.type != DataType.LIST) {
                                                throw new RuntimeException("WRONGTYPE");
                                            } else {
                                            ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.value;
                                                if (cmd.equals("LPUSH")) list.addFirst(val); else list.addLast(val);
                                                v.touch(); // Update LRU
                                                return v;
                                            }
                                        });
                                        aofHandler.log(cmd, key, val);
                                        checkBlockers(key); // Notify waiters
                                        ValueEntry v = store.get(key);
                                        int size = (v != null && v.value instanceof List) ? ((List)v.value).size() : 0;
                                        send(out, isResp, Resp.integer(size), "(integer) " + size);
                                    } catch (RuntimeException e) {
                                        send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                                    }
                                }
                                break;
                            
                            case "BLPOP":
                            case "BRPOP":
                                if (parts.size() < 3) send(out, isResp, Resp.error("usage: "+cmd+" key [key ...] timeout"), "(error) usage: "+cmd+" key [key ...] timeout");
                                else {
                                    try {
                                        double timeout = Double.parseDouble(parts.get(parts.size()-1));
                                        List<String> keys = parts.subList(1, parts.size()-1);
                                        
                                        boolean served = false;
                                        for (String k : keys) {
                                            ValueEntry entry = store.get(k);
                                            if (entry != null && entry.type == DataType.LIST) {
                                                ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) entry.value;
                                                if (!list.isEmpty()) {
                                                    String val = cmd.equals("BLPOP") ? list.pollFirst() : list.pollLast();
                                                    if (val != null) {
                                                        aofHandler.log(cmd.equals("BLPOP") ? "LPOP" : "RPOP", k);
                                                        if (list.isEmpty()) store.remove(k);
                                                        send(out, isResp, Resp.array(Arrays.asList(k, val)), null);
                                                        served = true;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        
                                        if (!served) {
                                            BlockingRequest bReq = new BlockingRequest(cmd.equals("BLPOP"));
                                            for (String k : keys) {
                                                blockingRegistry.computeIfAbsent(k, x -> new ConcurrentLinkedQueue<>()).add(bReq);
                                            }
                                            
                                            try {
                                                List<String> result;
                                                if (timeout <= 0) {
                                                    result = bReq.future.get(); 
                                                } else {
                                                     result = bReq.future.get((long)(timeout * 1000), TimeUnit.MILLISECONDS);
                                                }
                                                send(out, isResp, Resp.array(result), null);
                                            } catch (TimeoutException e) {
                                                bReq.future.cancel(true);
                                                send(out, isResp, Resp.bulkString(null), "(nil)");
                                            } catch (Exception e) {
                                                bReq.future.cancel(true);
                                                send(out, isResp, Resp.bulkString(null), "(nil)");
                                            }
                                        }
                                    } catch (NumberFormatException e) {
                                        send(out, isResp, Resp.error("ERR timeout is not a float or out of range"), "(error) ERR timeout is not a float or out of range");
                                    }
                                }
                                break;

                            case "LPOP":
                            case "RPOP":
                                if (parts.size() < 2) send(out, isResp, Resp.error("usage: "+cmd+" key"), "(error) usage: "+cmd+" key");
                                else {
                                    String key = parts.get(1);
                                    ValueEntry entry = store.get(key);
                                    if (entry == null) send(out, isResp, Resp.bulkString(null), "(nil)");
                                    else if (entry.type != DataType.LIST) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                                    else {
                                        ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) entry.value;
                                        if (list.isEmpty()) send(out, isResp, Resp.bulkString(null), "(nil)");
                                        else {
                                            String val = cmd.equals("LPOP") ? list.pollFirst() : list.pollLast(); // poll is safe
                                            if (val != null) {
                                                aofHandler.log(cmd, key);
                                                if (list.isEmpty()) store.remove(key); // Remove empty list
                                                send(out, isResp, Resp.bulkString(val), val);
                                            } else {
                                                send(out, isResp, Resp.bulkString(null), "(nil)");
                                            }
                                        }
                                    }
                                }
                                break;
                                
                            case "LRANGE":
                                if (parts.size() < 4) send(out, isResp, Resp.error("usage: LRANGE key start stop"), "(error) usage: LRANGE key start stop");
                                else {
                                    String key = parts.get(1);
                                    ValueEntry entry = store.get(key);
                                    if (entry == null) send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                                    else if (entry.type != DataType.LIST) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                                    else {
                                        ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) entry.value;
                                        int size = list.size(); // Approximate size
                                        int start = Integer.parseInt(parts.get(2));
                                        int end = Integer.parseInt(parts.get(3));
                                        
                                        if (start < 0) start += size;
                                        if (end < 0) end += size;
                                        if (start < 0) start = 0;
                                        // if (end >= size) end = size - 1; // Iterator handles this naturally
                                        
                                        List<String> sub = new ArrayList<>();
                                        if (start <= end) {
                                            Iterator<String> it = list.iterator();
                                            int idx = 0;
                                            while (it.hasNext() && idx <= end) {
                                                String s = it.next();
                                                if (idx >= start) {
                                                    sub.add(s);
                                                }
                                                idx++;
                                            }
                                        }
                                        if (isResp) send(out, true, Resp.array(sub), null);
                                        else {
                                            StringBuilder sb = new StringBuilder();
                                            for (int i = 0; i < sub.size(); i++) sb.append((i+1) + ") \"" + sub.get(i) + "\"\n");
                                            send(out, false, null, sb.toString().trim());
                                        }
                                    }
                                }
                                break;

                            // --- HASHES ---
                            case "HSET":
                                if (parts.size() < 4) send(out, isResp, Resp.error("usage: HSET key field value"), "(error) usage: HSET key field value");
                                else {
                                    performEvictionIfNeeded();
                                    String key = parts.get(1);
                                    String field = parts.get(2);
                                    String val = parts.get(3);
                                    final int[] ret = {0}; // 1 if new, 0 if updated
                                    try {
                                        store.compute(key, (k, v) -> {
                                            if (v == null) {
                                                ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
                                                map.put(field, val);
                                                ret[0] = 1;
                                                return new ValueEntry(map, DataType.HASH, -1);
                                            } else if (v.type != DataType.HASH) {
                                                throw new RuntimeException("WRONGTYPE");
                                            } else {
                                                ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.value;
                                                if (map.put(field, val) == null) ret[0] = 1;
                                                else ret[0] = 0;
                                                v.touch();
                                                return v;
                                            }
                                        });
                                        aofHandler.log("HSET", key, field, val);
                                        send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                                    } catch (RuntimeException e) {
                                        send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                                    }
                                }
                                break;
                                
                            case "HGET":
                                if (parts.size() < 3) send(out, isResp, Resp.error("usage: HGET key field"), "(error) usage: HGET key field");
                                else {
                                    String key = parts.get(1);
                                    ValueEntry entry = store.get(key);
                                    if (entry == null || entry.type != DataType.HASH) send(out, isResp, Resp.bulkString(null), "(nil)");
                                    else {
                                        ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) entry.value;
                                        String val = map.get(parts.get(2));
                                        send(out, isResp, Resp.bulkString(val), val != null ? val : "(nil)");
                                    }
                                }
                                break;
                                
                            case "HGETALL":
                                if (parts.size() < 2) send(out, isResp, Resp.error("usage: HGETALL key"), "(error) usage: HGETALL key");
                                else {
                                    String key = parts.get(1);
                                    ValueEntry entry = store.get(key);
                                    if (entry == null || entry.type != DataType.HASH) send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                                    else {
                                        ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) entry.value;
                                        List<String> flat = new ArrayList<>();
                                        for (Map.Entry<String, String> e : map.entrySet()) {
                                            flat.add(e.getKey());
                                            flat.add(e.getValue());
                                        }
                                        if (isResp) send(out, true, Resp.array(flat), null);
                                        else {
                                            StringBuilder sb = new StringBuilder();
                                            for (int i = 0; i < flat.size(); i+=2) {
                                                sb.append((i/2+1) + ") \"" + flat.get(i) + "\"\n");
                                                sb.append((i/2+1) + ") \"" + flat.get(i+1) + "\"\n"); // Wait, standard redis output is linear
                                            }
                                            // Actually redis cli formats it, raw it's just lines.
                                            // Simple text format:
                                            StringBuilder sb2 = new StringBuilder();
                                            for (int i = 0; i < flat.size(); i++) {
                                                 sb2.append((i+1) + ") \"" + flat.get(i) + "\"\n");
                                            }
                                            send(out, false, null, sb2.toString().trim());
                                        }
                                    }
                                }
                                break;
                            
                            case "HDEL":
                                if (parts.size() < 3) send(out, isResp, Resp.error("usage: HDEL key field"), "(error) usage: HDEL key field");
                                else {
                                    String key = parts.get(1);
                                    String field = parts.get(2);
                                    final int[] ret = {0};
                                    store.computeIfPresent(key, (k, v) -> {
                                        if (v.type == DataType.HASH) {
                                            ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.value;
                                            if (map.remove(field) != null) ret[0] = 1;
                                            if (map.isEmpty()) return null;
                                        }
                                        return v;
                                    });
                                    if (ret[0] == 1) aofHandler.log("HDEL", key, field);
                                    send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                                }
                                break;

                            // --- SETS ---
                            case "SADD":
                                if (parts.size() < 3) send(out, isResp, Resp.error("usage: SADD key member"), "(error) usage: SADD key member");
                                else {
                                    performEvictionIfNeeded();
                                    String key = parts.get(1);
                                    String member = parts.get(2);
                                    final int[] ret = {0};
                                    try {
                                        store.compute(key, (k, v) -> {
                                            if (v == null) {
                                                Set<String> set = ConcurrentHashMap.newKeySet();
                                                set.add(member);
                                                ret[0] = 1;
                                                return new ValueEntry(set, DataType.SET, -1);
                                            } else if (v.type != DataType.SET) {
                                                throw new RuntimeException("WRONGTYPE");
                                            } else {
                                                Set<String> set = (Set<String>) v.value;
                                                if (set.add(member)) ret[0] = 1;
                                                else ret[0] = 0;
                                                v.touch();
                                                return v;
                                            }
                                        });
                                        aofHandler.log("SADD", key, member);
                                        send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                                    } catch (RuntimeException e) {
                                        send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                                    }
                                }
                                break;
                                
                            case "SMEMBERS":
                                if (parts.size() < 2) send(out, isResp, Resp.error("usage: SMEMBERS key"), "(error) usage: SMEMBERS key");
                                else {
                                    String key = parts.get(1);
                                    ValueEntry entry = store.get(key);
                                    if (entry == null || entry.type != DataType.SET) send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                                    else {
                                        Set<String> set = (Set<String>) entry.value;
                                        List<String> list = new ArrayList<>(set);
                                        if (isResp) send(out, true, Resp.array(list), null);
                                        else {
                                            StringBuilder sb = new StringBuilder();
                                            for (int i = 0; i < list.size(); i++) sb.append((i+1) + ") \"" + list.get(i) + "\"\n");
                                            send(out, false, null, sb.toString().trim());
                                        }
                                    }
                                }
                                break;

                            case "SREM":
                                if (parts.size() < 3) send(out, isResp, Resp.error("usage: SREM key member"), "(error) usage: SREM key member");
                                else {
                                    String key = parts.get(1);
                                    String member = parts.get(2);
                                    final int[] ret = {0};
                                    store.computeIfPresent(key, (k, v) -> {
                                        if (v.type == DataType.SET) {
                                            Set<String> set = (Set<String>) v.value;
                                            if (set.remove(member)) ret[0] = 1;
                                            if (set.isEmpty()) return null;
                                        }
                                        return v;
                                    });
                                    if (ret[0] == 1) aofHandler.log("SREM", key, member);
                                    send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                                }
                                break;

                            case "DEL":
                                if (parts.size() < 2) send(out, isResp, Resp.error("usage: DEL key"), "(error) usage: DEL key");
                                else { 
                                    String key = parts.get(1);
                                    ValueEntry prev = store.remove(key);
                                    if (prev != null) aofHandler.log("DEL", key);
                                    send(out, isResp, Resp.integer(prev != null ? 1 : 0), "(integer) " + (prev != null ? 1 : 0));
                                }
                                break;
                            
                            // --- SORTED SETS ---
                            case "ZADD":
                                if (parts.size() < 4 || (parts.size() - 2) % 2 != 0) {
                                     send(out, isResp, Resp.error("usage: ZADD key score member [score member ...]"), "(error) usage: ZADD key score member ...");
                                } else {
                                     performEvictionIfNeeded();
                                     String key = parts.get(1);
                                     final int[] addedCount = {0};
                                     try {
                                         store.compute(key, (k, v) -> {
                                             CaradeZSet zset;
                                             if (v == null) {
                                                 zset = new CaradeZSet();
                                                 v = new ValueEntry(zset, DataType.ZSET, -1);
                                             } else if (v.type != DataType.ZSET) {
                                                 throw new RuntimeException("WRONGTYPE");
                                             } else {
                                                 zset = (CaradeZSet) v.value;
                                             }
                                             
                                             for (int i = 2; i < parts.size(); i += 2) {
                                                 try {
                                                     double score = Double.parseDouble(parts.get(i));
                                                     String member = parts.get(i+1);
                                                     addedCount[0] += zset.add(score, member);
                                                     // Log each pair? Or log bulk?
                                                     // AOF logging is outside compute usually, but we need correct args.
                                                 } catch (NumberFormatException e) {
                                                     throw new RuntimeException("ERR value is not a valid float");
                                                 }
                                             }
                                             v.touch();
                                             return v;
                                         });
                                         
                                         // Log ZADD
                                         // parts: ZADD, key, score, member, ...
                                         // Log as is
                                         String[] logArgs = parts.toArray(new String[0]);
                                         // But aofHandler.log takes (cmd, args...)
                                         // So pass array starting from index 1?
                                         // Actually aofHandler.log(cmd, args...)
                                         String[] args = new String[parts.size()-1];
                                         for(int i=1; i<parts.size(); i++) args[i-1] = parts.get(i);
                                         aofHandler.log("ZADD", args);
                                         
                                         send(out, isResp, Resp.integer(addedCount[0]), "(integer) " + addedCount[0]);
                                     } catch (RuntimeException e) {
                                         String msg = e.getMessage();
                                         if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                                             send(out, isResp, Resp.error(msg), "(error) " + msg);
                                         else throw e;
                                     }
                                }
                                break;
                                
                            case "ZRANGE":
                                if (parts.size() < 4) send(out, isResp, Resp.error("usage: ZRANGE key start stop [WITHSCORES]"), "(error) usage: ZRANGE key start stop [WITHSCORES]");
                                else {
                                    String key = parts.get(1);
                                    boolean withScores = parts.size() > 4 && parts.get(parts.size()-1).equalsIgnoreCase("WITHSCORES");
                                    
                                    ValueEntry entry = store.get(key);
                                    if (entry == null || entry.type != DataType.ZSET) {
                                        if (entry != null && entry.type != DataType.ZSET) {
                                            send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                                        } else {
                                            send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                                        }
                                    } else {
                                        try {
                                            int start = Integer.parseInt(parts.get(2));
                                            int end = Integer.parseInt(parts.get(3));
                                            CaradeZSet zset = (CaradeZSet) entry.value;
                                            int size = zset.size();
                                            
                                            if (start < 0) start += size;
                                            if (end < 0) end += size;
                                            if (start < 0) start = 0;
                                            // if (end >= size) end = size - 1; // handled by loop
                                            
                                            List<String> result = new ArrayList<>();
                                            if (start <= end) {
                                                Iterator<ZNode> it = zset.sorted.iterator();
                                                int idx = 0;
                                                while (it.hasNext() && idx <= end) {
                                                    ZNode node = it.next();
                                                    if (idx >= start) {
                                                        result.add(node.member);
                                                        if (withScores) {
                                                            // Format double to string, remove trailing .0 if integer?
                                                            // Redis standard behavior varies, usually just string rep.
                                                            // Java Double.toString() is fine.
                                                            String s = String.valueOf(node.score);
                                                            if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                                                            result.add(s);
                                                        }
                                                    }
                                                    idx++;
                                                }
                                            }
                                            
                                            if (isResp) send(out, true, Resp.array(result), null);
                                            else {
                                                 StringBuilder sb = new StringBuilder();
                                                 for (int i = 0; i < result.size(); i++) {
                                                     sb.append((i+1) + ") \"" + result.get(i) + "\"\n");
                                                 }
                                                 send(out, false, null, sb.toString().trim());
                                            }
                                            
                                        } catch (NumberFormatException e) {
                                            send(out, isResp, Resp.error("ERR value is not an integer or out of range"), "(error) ERR value is not an integer or out of range");
                                        }
                                    }
                                }
                                break;
                                
                            case "ZRANK":
                                if (parts.size() < 3) send(out, isResp, Resp.error("usage: ZRANK key member"), "(error) usage: ZRANK key member");
                                else {
                                    String key = parts.get(1);
                                    String member = parts.get(2);
                                    ValueEntry entry = store.get(key);
                                    if (entry == null || entry.type != DataType.ZSET) {
                                         if (entry != null) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                                         else send(out, isResp, Resp.bulkString(null), "(nil)");
                                    } else {
                                        CaradeZSet zset = (CaradeZSet) entry.value;
                                        Double score = zset.score(member);
                                        if (score == null) {
                                            send(out, isResp, Resp.bulkString(null), "(nil)");
                                        } else {
                                            // O(N) scan
                                            int rank = 0;
                                            for (ZNode node : zset.sorted) {
                                                if (node.member.equals(member)) break;
                                                rank++;
                                            }
                                            send(out, isResp, Resp.integer(rank), "(integer) " + rank);
                                        }
                                    }
                                }
                                break;

                            // --- NEW PUB/SUB COMMANDS ---
                            case "SUBSCRIBE":
                                if (parts.size() < 2) send(out, isResp, Resp.error("usage: SUBSCRIBE channel"), "(error) usage: SUBSCRIBE channel");
                                else {
                                    for (int i = 1; i < parts.size(); i++) {
                                        String channel = parts.get(i);
                                        pubSub.subscribe(channel, this);
                                        isSubscribed = true;
                                        if (isResp) {
                                            List<String> resp = Arrays.asList("subscribe", channel, "1"); // Actually 3rd arg is count of subscriptions
                                            send(out, true, Resp.array(resp), null);
                                        } else {
                                            send(out, false, null, "Subscribed to channel: " + channel);
                                        }
                                    }
                                }
                                break;
                            
                            case "UNSUBSCRIBE":
                                if (parts.size() < 2) {
                                    // Unsubscribe all
                                    pubSub.unsubscribeAll(this);
                                    if (isSubscribed) {
                                        isSubscribed = false; 
                                        // But wait, if we still have patterns? 
                                        // Simplified: assume 0 subs.
                                        // Redis replies with unsubscribe count 0
                                        if (isResp) send(out, true, Resp.array(Arrays.asList("unsubscribe", null, "0")), null);
                                        else send(out, false, null, "Unsubscribed from all");
                                    }
                                } else {
                                    for (int i = 1; i < parts.size(); i++) {
                                        String channel = parts.get(i);
                                        pubSub.unsubscribe(channel, this);
                                        if (isResp) send(out, true, Resp.array(Arrays.asList("unsubscribe", channel, "0")), null); 
                                        else send(out, false, null, "Unsubscribed from: " + channel);
                                    }
                                    // If no more subs?
                                    // Simplified logic: keep isSubscribed true until all gone?
                                    // For now user just wants UNSUBSCRIBE ability.
                                }
                                break;

                            case "PSUBSCRIBE":
                                if (parts.size() < 2) send(out, isResp, Resp.error("usage: PSUBSCRIBE pattern"), "(error) usage: PSUBSCRIBE pattern");
                                else {
                                    for (int i = 1; i < parts.size(); i++) {
                                        String pattern = parts.get(i);
                                        pubSub.psubscribe(pattern, this);
                                        isSubscribed = true;
                                        if (isResp) {
                                            List<String> resp = Arrays.asList("psubscribe", pattern, "1");
                                            send(out, true, Resp.array(resp), null);
                                        } else {
                                            send(out, false, null, "Subscribed to pattern: " + pattern);
                                        }
                                    }
                                }
                                break;
                                
                            case "PUNSUBSCRIBE":
                                // Simplified implementation
                                if (parts.size() >= 2) {
                                     for (int i = 1; i < parts.size(); i++) {
                                        String pattern = parts.get(i);
                                        pubSub.punsubscribe(pattern, this);
                                        if (isResp) send(out, true, Resp.array(Arrays.asList("punsubscribe", pattern, "0")), null); 
                                        else send(out, false, null, "Unsubscribed from pattern: " + pattern);
                                     }
                                }
                                break;

                            case "PUBLISH":
                                if (parts.size() < 3) send(out, isResp, Resp.error("usage: PUBLISH channel message"), "(error) usage: PUBLISH channel message");
                                else {
                                    String channel = parts.get(1);
                                    String msg = parts.get(2);
                                    int count = pubSub.publish(channel, msg);
                                    send(out, isResp, Resp.integer(count), "(integer) " + count);
                                }
                                break;

                            case "INFO":
                                StringBuilder info = new StringBuilder();
                                info.append("# Server\n");
                                info.append("carade_version:0.1.0\n");
                                info.append("tcp_port:").append(config.port).append("\n");
                                info.append("uptime_in_seconds:").append(ManagementFactory.getRuntimeMXBean().getUptime() / 1000).append("\n");
                                info.append("\n# Clients\n");
                                info.append("connected_clients:").append(activeConnections.get()).append("\n");
                                info.append("\n# Memory\n");
                                info.append("used_memory:").append(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()).append("\n");
                                info.append("maxmemory:").append(config.maxMemory).append("\n");
                                info.append("\n# Stats\n");
                                info.append("total_commands_processed:").append(totalCommands.get()).append("\n");
                                info.append("keyspace_hits:0\n"); // Todo
                                info.append("keyspace_misses:0\n"); // Todo
                                info.append("\n# Persistence\n");
                                info.append("aof_enabled:1\n");
                                
                                if (isResp) send(out, true, Resp.bulkString(info.toString()), null);
                                else send(out, false, null, info.toString());
                                break;
                            case "DBSIZE": send(out, isResp, Resp.integer(store.size()), "(integer) " + store.size()); break;
                            case "FLUSHALL": 
                                store.clear(); 
                                aofHandler.log("FLUSHALL");
                                send(out, isResp, Resp.simpleString("OK"), "OK"); 
                                break;
                            case "BGREWRITEAOF":
                                // Execute in background
                                CompletableFuture.runAsync(() -> {
                                    System.out.println("🔄 Starting Background AOF Rewrite...");
                                    aofHandler.rewrite(store);
                                });
                                send(out, isResp, Resp.simpleString("Background append only file rewriting started"), "Background append only file rewriting started");
                                break;
                            case "PING": send(out, isResp, Resp.simpleString("PONG"), "PONG"); break;
                            case "QUIT": socket.close(); return;
                            default: send(out, isResp, Resp.error("ERR unknown command"), "(error) ERR unknown command");
                        }
                    } catch (Exception e) { send(out, isResp, Resp.error("ERR " + e.getMessage()), "(error) ERR " + e.getMessage()); }
                }
            } catch (IOException e) { 
                // Client disconnected
            } finally {
                // Cleanup Pub/Sub
                pubSub.unsubscribeAll(this);
                activeConnections.decrementAndGet();
            }
        }
    }
}