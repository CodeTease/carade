import java.lang.management.ManagementFactory;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.nio.charset.StandardCharsets;

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

         double incrBy(double increment, String member) {
             Double oldScore = scores.get(member);
             double newScore = (oldScore == null ? 0 : oldScore) + increment;
             add(newScore, member);
             return newScore;
         }
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
        
        boolean isExpired() { return expireAt != -1 && System.currentTimeMillis() > expireAt; }
        
        void touch() { this.lastAccessed = System.nanoTime(); }
    }

    private static ConcurrentHashMap<String, ValueEntry> store = new ConcurrentHashMap<>();
    private static final ReentrantReadWriteLock globalRWLock = new ReentrantReadWriteLock();
    private static AofHandler aofHandler;
    
    // --- SCAN ENGINE ---
    static class ScanCursor {
        final Iterator<?> iterator;
        final DataType type; // For verification if needed, or context
        volatile long lastAccess;
        ScanCursor(Iterator<?> iterator, DataType type) {
            this.iterator = iterator;
            this.type = type;
            this.lastAccess = System.currentTimeMillis();
        }
    }
    private static final ConcurrentHashMap<String, ScanCursor> scanRegistry = new ConcurrentHashMap<>();
    private static final AtomicLong cursorIdGen = new AtomicLong(1);

    // --- WATCH / TRANSACTIONS ---
    private static final ConcurrentHashMap<String, Set<ClientHandler>> watchers = new ConcurrentHashMap<>();

    static void notifyWatchers(String key) {
        Set<ClientHandler> interested = watchers.get(key);
        if (interested != null) {
            for (ClientHandler client : interested) {
                client.markDirty();
            }
        }
    }

    // --- BLOCKING QUEUES ---
    static class BlockingRequest {
        final CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
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
                    boolean completed = req.future.complete(Arrays.asList(key.getBytes(StandardCharsets.UTF_8), val.getBytes(StandardCharsets.UTF_8)));
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
        aofHandler.replay(cmd -> executeInternal(cmd));

        // 1. Janitor
        Thread janitor = new Thread(() -> {
            while (isRunning) {
                try {
                    Thread.sleep(30000); 
                    cleanupExpiredKeys(); 
                    cleanupExpiredCursors();
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
    private static final AtomicInteger writeCounter = new AtomicInteger(0);

    private static void performEvictionIfNeeded() {
        if (config.maxMemory <= 0) return;
        if (writeCounter.incrementAndGet() % 50 != 0) return;
        long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (used < config.maxMemory) return;
        
        System.out.println("🧹 [Eviction] Memory full (" + (used/1024/1024) + "MB > " + (config.maxMemory/1024/1024) + "MB). Evicting...");
        
        int attempts = 0;
        Iterator<String> it = store.keySet().iterator();
        while (used > config.maxMemory && !store.isEmpty() && attempts < 100) {
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
            if (!it.hasNext()) it = store.keySet().iterator();
            
            if (bestKey != null) {
                store.remove(bestKey);
                if (aofHandler != null) aofHandler.log("DEL", bestKey);
            }
            
            used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            attempts++;
        }
    }

    private static void executeInternal(List<byte[]> parts) {
        if (parts.isEmpty()) return;
        String cmd = new String(parts.get(0), StandardCharsets.UTF_8).toUpperCase();
        try {
            switch (cmd) {
                case "SET":
                    if (parts.size() >= 3) {
                        long ttl = -1;
                        if (parts.size() >= 5 && new String(parts.get(3), StandardCharsets.UTF_8).equalsIgnoreCase("EX")) {
                            try { ttl = Long.parseLong(new String(parts.get(4), StandardCharsets.UTF_8)); } catch (Exception e) {}
                        }
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        store.put(key, new ValueEntry(parts.get(2), DataType.STRING, ttl));
                    }
                    break;
                case "SETBIT":
                    // To be strictly correct with byte arrays we should adapt this, but for now we keep string based logic where acceptable
                    if (parts.size() >= 4) {
                         String key = new String(parts.get(1), StandardCharsets.UTF_8);
                         try {
                             int offset = Integer.parseInt(new String(parts.get(2), StandardCharsets.UTF_8));
                             int val = Integer.parseInt(new String(parts.get(3), StandardCharsets.UTF_8));
                             store.compute(key, (k, v) -> {
                                 byte[] bytes;
                                 if (v == null) bytes = new byte[0];
                                 else if (v.type == DataType.STRING) bytes = (byte[]) v.value;
                                 else return v;
                                 
                                 int byteIndex = offset / 8;
                                 if (byteIndex >= bytes.length) {
                                     byte[] newBytes = new byte[byteIndex + 1];
                                     System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                                     bytes = newBytes;
                                 }
                                 
                                 if (val == 1) bytes[byteIndex] |= (1 << (7 - (offset % 8)));
                                 else bytes[byteIndex] &= ~(1 << (7 - (offset % 8)));
                                 
                                 ValueEntry newV = new ValueEntry(bytes, DataType.STRING, -1);
                                 if (v != null) newV.expireAt = v.expireAt;
                                 return newV;
                             });
                         } catch (Exception e) {}
                    }
                    break;
                case "RENAME":
                    if (parts.size() >= 3) {
                         String oldKey = new String(parts.get(1), StandardCharsets.UTF_8);
                         String newKey = new String(parts.get(2), StandardCharsets.UTF_8);
                         ValueEntry v = store.remove(oldKey);
                         if (v != null) store.put(newKey, v);
                    }
                    break;
                case "DEL":
                    if (parts.size() >= 2) store.remove(new String(parts.get(1), StandardCharsets.UTF_8));
                    break;
                case "HDEL":
                    if (parts.size() >= 3) {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String field = new String(parts.get(2), StandardCharsets.UTF_8);
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
                         String key = new String(parts.get(1), StandardCharsets.UTF_8);
                         String member = new String(parts.get(2), StandardCharsets.UTF_8);
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
                case "ZREM":
                    if (parts.size() >= 3) {
                         String key = new String(parts.get(1), StandardCharsets.UTF_8);
                         String member = new String(parts.get(2), StandardCharsets.UTF_8);
                         store.computeIfPresent(key, (k, v) -> {
                            if (v.type == DataType.ZSET) {
                                CaradeZSet zset = (CaradeZSet) v.value;
                                Double score = zset.scores.remove(member);
                                if (score != null) {
                                     zset.sorted.remove(new ZNode(score, member));
                                }
                                if (zset.scores.isEmpty()) return null;
                            }
                            return v;
                        });
                    }
                    break;
                case "ZADD":
                    if (parts.size() >= 4) {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
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
                                         double score = Double.parseDouble(new String(parts.get(i), StandardCharsets.UTF_8));
                                         String member = new String(parts.get(i+1), StandardCharsets.UTF_8);
                                         zset.add(score, member);
                                     } catch (Exception ex) {}
                                 }
                                 return v;
                             });
                        } catch (Exception e) {}
                    }
                    break;
                case "ZINCRBY":
                    if (parts.size() >= 4) {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        try {
                             double incr = Double.parseDouble(new String(parts.get(2), StandardCharsets.UTF_8));
                             String member = new String(parts.get(3), StandardCharsets.UTF_8);
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
                                 zset.incrBy(incr, member);
                                 return v;
                             });
                        } catch (Exception e) {}
                    }
                    break;
                case "MSET":
                    if (parts.size() >= 3) {
                        for (int i = 1; i < parts.size(); i += 2) {
                            if (i + 1 < parts.size()) {
                                String key = new String(parts.get(i), StandardCharsets.UTF_8);
                                byte[] val = parts.get(i+1);
                                store.put(key, new ValueEntry(val, DataType.STRING, -1));
                            }
                        }
                    }
                    break;
                case "LPUSH":
                case "RPUSH":
                    if (parts.size() >= 3) {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String val = new String(parts.get(2), StandardCharsets.UTF_8);
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
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
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
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String field = new String(parts.get(2), StandardCharsets.UTF_8);
                        String val = new String(parts.get(3), StandardCharsets.UTF_8);
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
                case "HINCRBY":
                    if (parts.size() >= 4) {
                         String key = new String(parts.get(1), StandardCharsets.UTF_8);
                         String field = new String(parts.get(2), StandardCharsets.UTF_8);
                         try {
                             long incr = Long.parseLong(new String(parts.get(3), StandardCharsets.UTF_8));
                             store.compute(key, (k, v) -> {
                                if (v == null) {
                                    ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
                                    map.put(field, String.valueOf(incr));
                                    return new ValueEntry(map, DataType.HASH, -1);
                                } else if (v.type != DataType.HASH) {
                                    return v;
                                } else {
                                    ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.value;
                                    map.compute(field, (f, val) -> {
                                        long oldVal = 0;
                                        if (val != null) {
                                            try { oldVal = Long.parseLong(val); } catch (Exception e) {}
                                        }
                                        long newVal = oldVal + incr;
                                        return String.valueOf(newVal);
                                    });
                                    return v;
                                }
                            });
                         } catch (Exception e) {}
                    }
                    break;
                case "SADD":
                    if (parts.size() >= 3) {
                         String key = new String(parts.get(1), StandardCharsets.UTF_8);
                         String member = new String(parts.get(2), StandardCharsets.UTF_8);
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
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        store.compute(key, (k, v) -> {
                            long val = 0;
                            if (v == null) {
                                val = 0;
                            } else if (v.type == DataType.STRING) {
                                try {
                                    val = Long.parseLong(new String((byte[])v.value, java.nio.charset.StandardCharsets.UTF_8));
                                } catch (Exception e) { return v; } 
                            } else {
                                return v;
                            }
                            if (cmd.equals("INCR")) val++; else val--;
                            ValueEntry newV = new ValueEntry(String.valueOf(val).getBytes(java.nio.charset.StandardCharsets.UTF_8), DataType.STRING, -1);
                            if (v != null) newV.expireAt = v.expireAt;
                            return newV;
                        });
                    }
                    break;
                case "EXPIRE":
                    if (parts.size() >= 3) {
                         String key = new String(parts.get(1), StandardCharsets.UTF_8);
                         try {
                             long seconds = Long.parseLong(new String(parts.get(2), StandardCharsets.UTF_8));
                             store.computeIfPresent(key, (k, v) -> {
                                 v.expireAt = System.currentTimeMillis() + (seconds * 1000);
                                 return v;
                             });
                         } catch (Exception e) {}
                    }
                    break;
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
                if (aofHandler != null) aofHandler.log("DEL", entry.getKey());
                removed++;
            }
        }
        if (removed > 0) System.out.println("🧹 [GC] Vacuumed " + removed + " expired keys.");
    }
    
    private static void cleanupExpiredCursors() {
        long now = System.currentTimeMillis();
        scanRegistry.entrySet().removeIf(e -> (now - e.getValue().lastAccess) > 600000); // 10 mins
    }

    private static void saveData() {
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(DUMP_FILE)))) {
            dos.writeBytes("CARD");
            dos.writeInt(1);
            
            for (Map.Entry<String, ValueEntry> entry : store.entrySet()) {
                ValueEntry v = entry.getValue();
                if (v.isExpired()) continue;
                
                int typeCode = 0;
                if (v.type == DataType.LIST) typeCode = 1;
                else if (v.type == DataType.HASH) typeCode = 2;
                else if (v.type == DataType.SET) typeCode = 3;
                else if (v.type == DataType.ZSET) typeCode = 4;
                dos.writeByte(typeCode);
                
                dos.writeLong(v.expireAt);
                writeString(dos, entry.getKey());
                
                switch (v.type) {
                    case STRING:
                        // writeString expects String, but now we store byte[]
                        // Ideally we should write bytes directly
                        byte[] b = (byte[]) v.value;
                        dos.writeInt(b.length);
                        dos.write(b);
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
            byte[] magic = new byte[4];
            dis.readFully(magic);
            if (!Arrays.equals(magic, "CARD".getBytes())) {
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
                        String s = readString(dis);
                        value = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
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
                    ve.expireAt = expireAt;
                    if (!ve.isExpired()) store.put(key, ve);
                    
                } catch (EOFException e) { break; }
            }
            System.out.println("📂 Loaded " + store.size() + " keys (Snapshot).");
            
        } catch (Exception e) {
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
                        store.put(key, new ValueEntry(((String) val).getBytes(java.nio.charset.StandardCharsets.UTF_8), DataType.STRING, -1));
                    } else if (val instanceof ValueEntry) {
                        ValueEntry ve = (ValueEntry) val;
                        if (ve.type == DataType.STRING && ve.value instanceof String) {
                            ve.value = ((String) ve.value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        }
                        store.put(key, ve);
                    }
                }
                System.out.println("📂 Loaded " + store.size() + " keys (Legacy).");
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
        
        // Transaction State
        private boolean isInTransaction = false;
        private volatile boolean transactionDirty = false;
        private Set<String> watching = new HashSet<>();
        private List<List<byte[]>> transactionQueue = new ArrayList<>();

        public ClientHandler(Socket socket) { this.socket = socket; }

        public void markDirty() {
            this.transactionDirty = true;
        }

        private void unwatchAll() {
            if (watching.isEmpty()) return;
            for (String key : watching) {
                watchers.computeIfPresent(key, (k, list) -> {
                    list.remove(this);
                    return list.isEmpty() ? null : list;
                });
            }
            watching.clear();
        }

        private synchronized void send(OutputStream out, boolean isResp, byte[] respData, String textData) {
            try {
                if (isResp) {
                     if (respData != null) out.write(respData);
                }
                else {
                    if (textData != null) out.write((textData + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }
                out.flush();
            } catch (IOException e) {
                // Ignore, connection likely closed
            }
        }

        private boolean isWriteCommand(String cmd) {
            return Arrays.asList("SET", "DEL", "LPUSH", "RPUSH", "LPOP", "RPOP", 
                                 "HSET", "HDEL", "SADD", "SREM", "FLUSHALL", 
                                 "HINCRBY", "SISMEMBER", "SCARD", 
                                 "RENAME", "ZREM", "SETBIT").contains(cmd);
        }
        
        private boolean isAdminCommand(String cmd) {
            return Arrays.asList("FLUSHALL", "DBSIZE").contains(cmd);
        }

        @Override
        public void send(String channel, String message, String pattern) {
            // Callback from PubSub engine
            if (currentIsResp) {
                if (pattern != null) {
                    List<byte[]> resp = new ArrayList<>();
                    resp.add("pmessage".getBytes(StandardCharsets.UTF_8));
                    resp.add(pattern.getBytes(StandardCharsets.UTF_8));
                    resp.add(channel.getBytes(StandardCharsets.UTF_8));
                    resp.add(message.getBytes(StandardCharsets.UTF_8));
                    send(outStream, true, Resp.array(resp), null);
                } else {
                    List<byte[]> resp = new ArrayList<>();
                    resp.add("message".getBytes(StandardCharsets.UTF_8));
                    resp.add(channel.getBytes(StandardCharsets.UTF_8));
                    resp.add(message.getBytes(StandardCharsets.UTF_8));
                    send(outStream, true, Resp.array(resp), null);
                }
            } else {
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
                    List<byte[]> parts = req.args;
                    this.currentIsResp = req.isResp;
                    boolean isResp = req.isResp;

                    if (parts.isEmpty()) continue;
                    totalCommands.incrementAndGet();

                    String cmd = new String(parts.get(0), StandardCharsets.UTF_8).toUpperCase();

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
                    
                    if (currentUser != null && !currentUser.canWrite && isWriteCommand(cmd)) {
                         send(out, isResp, Resp.error("ERR permission denied"), "(error) ERR permission denied");
                         continue;
                    }
                    if (currentUser != null && !currentUser.isAdmin && isAdminCommand(cmd)) {
                         send(out, isResp, Resp.error("ERR permission denied"), "(error) ERR permission denied");
                         continue;
                    }

                    try {
                        // Handle Transactions
                        if (cmd.equals("MULTI")) {
                            if (isInTransaction) {
                                send(out, isResp, Resp.error("ERR MULTI calls can not be nested"), "(error) ERR MULTI calls can not be nested");
                            } else {
                                isInTransaction = true;
                                transactionQueue.clear();
                                send(out, isResp, Resp.simpleString("OK"), "OK");
                            }
                            continue;
                        } else if (cmd.equals("DISCARD")) {
                            if (!isInTransaction) {
                                send(out, isResp, Resp.error("ERR DISCARD without MULTI"), "(error) ERR DISCARD without MULTI");
                            } else {
                                isInTransaction = false;
                                transactionQueue.clear();
                                unwatchAll(); // WATCH state is cleared on DISCARD
                                transactionDirty = false;
                                send(out, isResp, Resp.simpleString("OK"), "OK");
                            }
                            continue;
                        } else if (cmd.equals("EXEC")) {
                            if (!isInTransaction) {
                                send(out, isResp, Resp.error("ERR EXEC without MULTI"), "(error) ERR EXEC without MULTI");
                            } else {
                                isInTransaction = false;
                                
                                if (transactionDirty) {
                                    send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                                    transactionQueue.clear();
                                    transactionDirty = false;
                                    unwatchAll();
                                    continue;
                                }

                                unwatchAll(); // EXEC unwatch all keys
                                transactionDirty = false;
                                
                                if (transactionQueue.isEmpty()) {
                                    send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                                } else {
                                    // Acquire write lock to ensure exclusive execution
                                    globalRWLock.writeLock().lock();
                                    try {
                                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                                        // Write array header
                                        String header = "*" + transactionQueue.size() + "\r\n";
                                        buffer.write(header.getBytes());
                                        
                                        for (List<byte[]> queuedCmd : transactionQueue) {
                                            executeCommand(queuedCmd, buffer, isResp);
                                        }
                                        out.write(buffer.toByteArray());
                                        out.flush();
                                    } finally {
                                        globalRWLock.writeLock().unlock();
                                    }
                                }
                            }
                            continue;
                        } else if (cmd.equals("WATCH")) {
                            if (isInTransaction) {
                                send(out, isResp, Resp.error("ERR WATCH inside MULTI is not allowed"), "(error) ERR WATCH inside MULTI is not allowed");
                            } else {
                                if (parts.size() < 2) {
                                    send(out, isResp, Resp.error("usage: WATCH key [key ...]"), "(error) usage: WATCH key [key ...]");
                                } else {
                                    for (int i = 1; i < parts.size(); i++) {
                                        String key = new String(parts.get(i), StandardCharsets.UTF_8);
                                        watching.add(key);
                                        watchers.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(this);
                                    }
                                    send(out, isResp, Resp.simpleString("OK"), "OK");
                                }
                            }
                            continue;
                        } else if (cmd.equals("UNWATCH")) {
                            unwatchAll();
                            transactionDirty = false;
                            send(out, isResp, Resp.simpleString("OK"), "OK");
                            continue;
                        }

                        if (isInTransaction && !cmd.equals("AUTH") && !cmd.equals("QUIT")) {
                            transactionQueue.add(parts);
                            send(out, isResp, Resp.simpleString("QUEUED"), "QUEUED");
                            continue;
                        }

                        // Identify if command needs exclusive write lock (Global Lock)
                        boolean needsExclusive = Arrays.asList("RENAME", "MSET", "FLUSHALL", "BGREWRITEAOF").contains(cmd);
                        
                        if (needsExclusive) {
                            globalRWLock.writeLock().lock();
                            try {
                                executeCommand(parts, out, isResp);
                            } finally {
                                globalRWLock.writeLock().unlock();
                            }
                        } else {
                            // Acquire read lock for normal commands (concurrent)
                            globalRWLock.readLock().lock();
                            try {
                                executeCommand(parts, out, isResp);
                            } finally {
                                globalRWLock.readLock().unlock();
                            }
                        }
                        
                        if (cmd.equals("QUIT")) return;

                    } catch (Exception e) { send(out, isResp, Resp.error("ERR " + e.getMessage()), "(error) ERR " + e.getMessage()); }
                }
            } catch (IOException e) { 
                // Client disconnected
            } finally {
                // Cleanup Pub/Sub
                pubSub.unsubscribeAll(this);
                unwatchAll();
                activeConnections.decrementAndGet();
            }
        }
        private void handleScan(List<byte[]> parts, OutputStream out, boolean isResp, String cmd, String key) throws IOException {
             // Parse generic args: cursor [MATCH pattern] [COUNT count]
             // For SCAN: parts[1] is cursor.
             // For XSCAN: parts[2] is cursor.
             int cursorIdx = cmd.equals("SCAN") ? 1 : 2;
             if (parts.size() <= cursorIdx) {
                 send(out, isResp, Resp.error("wrong number of arguments for '" + cmd.toLowerCase() + "' command"), "(error) wrong number of arguments");
                 return;
             }
             
             String cursor = new String(parts.get(cursorIdx), StandardCharsets.UTF_8);
             String pattern = null;
             int count = 10;
             
             for (int i = cursorIdx + 1; i < parts.size(); i++) {
                 String arg = new String(parts.get(i), StandardCharsets.UTF_8).toUpperCase();
                 if (arg.equals("MATCH") && i + 1 < parts.size()) {
                     pattern = new String(parts.get(++i), StandardCharsets.UTF_8);
                 } else if (arg.equals("COUNT") && i + 1 < parts.size()) {
                     try { count = Integer.parseInt(new String(parts.get(++i), StandardCharsets.UTF_8)); } catch (Exception e) {}
                 }
             }
             
             java.util.regex.Pattern regex = null;
             if (pattern != null) {
                 String r = pattern.replace(".", "\\.").replace("*", ".*").replace("?", ".");
                 regex = java.util.regex.Pattern.compile(r);
             }
             
             Iterator<?> it;
             ScanCursor sc = null;
             
             if (cursor.equals("0")) {
                 // New iterator
                 if (cmd.equals("SCAN")) {
                     it = store.keySet().iterator();
                 } else {
                     ValueEntry entry = store.get(key);
                     if (entry == null) {
                         send(out, isResp, Resp.array(Arrays.asList("0".getBytes(StandardCharsets.UTF_8), Resp.array(Collections.emptyList()))), null);
                         return;
                     }
                     if (cmd.equals("HSCAN") && entry.type == DataType.HASH) {
                         it = ((ConcurrentHashMap<String, String>)entry.value).entrySet().iterator();
                     } else if (cmd.equals("SSCAN") && entry.type == DataType.SET) {
                         it = ((Set<String>)entry.value).iterator();
                     } else if (cmd.equals("ZSCAN") && entry.type == DataType.ZSET) {
                         it = ((CaradeZSet)entry.value).scores.entrySet().iterator();
                     } else {
                         // Empty or wrong type
                         send(out, isResp, Resp.array(Arrays.asList("0".getBytes(StandardCharsets.UTF_8), Resp.array(Collections.emptyList()))), null);
                         return;
                     }
                 }
                 sc = new ScanCursor(it, null); // Type check handled above
                 String newCursor = String.valueOf(cursorIdGen.getAndIncrement());
                 scanRegistry.put(newCursor, sc);
                 cursor = newCursor;
             } else {
                 sc = scanRegistry.get(cursor);
                 if (sc == null) {
                      // Cursor not found (expired or invalid). Redis behavior: return 0 and empty list.
                      cursor = "0";
                      it = Collections.emptyIterator();
                 } else {
                      it = sc.iterator;
                      sc.lastAccess = System.currentTimeMillis();
                 }
             }
             
             List<byte[]> results = new ArrayList<>();
             int found = 0;
             while (it.hasNext() && found < count) {
                 Object next = it.next();
                 found++; // Count raw elements processed
                 
                 String k = null;
                 List<byte[]> entryBytes = new ArrayList<>();
                 
                 if (cmd.equals("SCAN") || cmd.equals("SSCAN")) {
                     k = (String) next;
                     if (regex == null || regex.matcher(k).matches()) {
                         results.add(k.getBytes(StandardCharsets.UTF_8));
                     }
                 } else if (cmd.equals("HSCAN")) {
                     Map.Entry<String, String> e = (Map.Entry<String, String>) next;
                     if (regex == null || regex.matcher(e.getKey()).matches()) {
                         results.add(e.getKey().getBytes(StandardCharsets.UTF_8));
                         results.add(e.getValue().getBytes(StandardCharsets.UTF_8));
                     }
                 } else if (cmd.equals("ZSCAN")) {
                     Map.Entry<String, Double> e = (Map.Entry<String, Double>) next;
                     if (regex == null || regex.matcher(e.getKey()).matches()) {
                         results.add(e.getKey().getBytes(StandardCharsets.UTF_8));
                         String s = String.valueOf(e.getValue());
                         if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                         results.add(s.getBytes(StandardCharsets.UTF_8));
                     }
                 }
             }
             
             if (!it.hasNext()) {
                 scanRegistry.remove(cursor);
                 cursor = "0";
             }
             
             if (isResp) {
                 List<byte[]> outer = new ArrayList<>();
                 outer.add(cursor.getBytes(StandardCharsets.UTF_8));
                 outer.add(Resp.array(results));
                 send(out, true, Resp.array(outer), null);
             } else {
                 // Simple text representation
                 StringBuilder sb = new StringBuilder();
                 sb.append("1) \"").append(cursor).append("\"\n");
                 sb.append("2) ");
                 for (int i=0; i<results.size(); i++) {
                     sb.append(i==0 ? "" : "\n   ").append(i+1).append(") \"").append(new String(results.get(i), StandardCharsets.UTF_8)).append("\"");
                 }
                 send(out, false, null, sb.toString());
             }
        }
        
        // Extracted command execution logic
        private void executeCommand(List<byte[]> parts, OutputStream out, boolean isResp) throws IOException {
            String cmd = new String(parts.get(0), StandardCharsets.UTF_8).toUpperCase();
            // Notify watchers for modification commands
            
            switch (cmd) {
                // --- SCAN COMMANDS ---
                case "SCAN":
                    handleScan(parts, out, isResp, "SCAN", null);
                    break;
                case "HSCAN":
                case "SSCAN":
                case "ZSCAN":
                    if (parts.size() < 2) {
                        send(out, isResp, Resp.error("usage: " + cmd + " key cursor [MATCH pattern] [COUNT count]"), "(error) usage: " + cmd + " key cursor ...");
                    } else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        handleScan(parts, out, isResp, cmd, key);
                    }
                    break;

                case "AUTH":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: AUTH [user] password"), "(error) usage: AUTH [user] password");
                    else {
                        String user = "default";
                        String pass = "";
                        if (parts.size() == 2) {
                            pass = new String(parts.get(1), StandardCharsets.UTF_8);
                        } else {
                            user = new String(parts.get(1), StandardCharsets.UTF_8);
                            pass = new String(parts.get(2), StandardCharsets.UTF_8);
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

                case "EXISTS":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: EXISTS key"), "(error) usage: EXISTS key");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                        if (entry == null || entry.isExpired()) {
                             if (entry != null && entry.isExpired()) store.remove(key);
                             send(out, isResp, Resp.integer(0), "(integer) 0");
                        } else {
                             send(out, isResp, Resp.integer(1), "(integer) 1");
                        }
                    }
                    break;
                
                case "TYPE":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: TYPE key"), "(error) usage: TYPE key");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                        if (entry == null || entry.isExpired()) {
                             if (entry != null && entry.isExpired()) store.remove(key);
                             send(out, isResp, Resp.simpleString("none"), "none");
                        } else {
                             send(out, isResp, Resp.simpleString(entry.type.name().toLowerCase()), entry.type.name().toLowerCase());
                        }
                    }
                    break;

                case "RENAME":
                    if (parts.size() < 3) send(out, isResp, Resp.error("usage: RENAME key newkey"), "(error) usage: RENAME key newkey");
                    else {
                        String oldKey = new String(parts.get(1), StandardCharsets.UTF_8);
                        String newKey = new String(parts.get(2), StandardCharsets.UTF_8);
                        
                        ValueEntry val = store.remove(oldKey);
                        if (val == null || val.isExpired()) {
                            if (val != null) store.remove(oldKey); // Cleanup
                            send(out, isResp, Resp.error("ERR no such key"), "(error) ERR no such key");
                        } else {
                            store.put(newKey, val);
                            notifyWatchers(oldKey);
                            notifyWatchers(newKey);
                            aofHandler.log("RENAME", oldKey, newKey);
                            send(out, isResp, Resp.simpleString("OK"), "OK");
                        }
                    }
                    break;

                case "SET":
                    if (parts.size() < 3) send(out, isResp, Resp.error("usage: SET key value [EX seconds]"), "(error) usage: SET key value [EX seconds]");
                    else {
                        performEvictionIfNeeded();
                        long ttl = -1;
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        byte[] val = parts.get(2);
                        if (parts.size() >= 5 && new String(parts.get(3), StandardCharsets.UTF_8).equalsIgnoreCase("EX")) {
                            try { ttl = Long.parseLong(new String(parts.get(4), StandardCharsets.UTF_8)); } catch (Exception e) {}
                        }
                        store.put(key, new ValueEntry(val, DataType.STRING, ttl));
                        notifyWatchers(key);
                        if (ttl > 0) aofHandler.log("SET", key, val, "EX", String.valueOf(ttl));
                        else aofHandler.log("SET", key, val);
                        
                        send(out, isResp, Resp.simpleString("OK"), "OK");
                    }
                    break;

                case "SETBIT":
                    if (parts.size() < 4) send(out, isResp, Resp.error("usage: SETBIT key offset value"), "(error) usage: SETBIT key offset value");
                    else {
                        performEvictionIfNeeded();
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        final int[] oldBit = {0};
                        try {
                            int offset = Integer.parseInt(new String(parts.get(2), StandardCharsets.UTF_8));
                            int val = Integer.parseInt(new String(parts.get(3), StandardCharsets.UTF_8));
                            if (val != 0 && val != 1) {
                                send(out, isResp, Resp.error("ERR bit is not an integer or out of range"), "(error) ERR bit is not an integer or out of range");
                            } else if (offset < 0) {
                                send(out, isResp, Resp.error("ERR bit offset is not an integer or out of range"), "(error) ERR bit offset is not an integer or out of range");
                            } else {
                                store.compute(key, (k, v) -> {
                                    byte[] bytes;
                                    if (v == null) bytes = new byte[0];
                                    else if (v.type != DataType.STRING) throw new RuntimeException("WRONGTYPE");
                                    else bytes = (byte[]) v.value;
                                    
                                    int byteIndex = offset / 8;
                                    int bitIndex = 7 - (offset % 8);
                                    
                                    if (byteIndex < bytes.length) {
                                        oldBit[0] = (bytes[byteIndex] >> bitIndex) & 1;
                                    } else {
                                        oldBit[0] = 0;
                                    }
                                    
                                    if (byteIndex >= bytes.length) {
                                        // Extend
                                        byte[] newBytes = new byte[byteIndex + 1];
                                        System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
                                        bytes = newBytes;
                                    }
                                    
                                    if (val == 1) bytes[byteIndex] |= (1 << bitIndex);
                                    else bytes[byteIndex] &= ~(1 << bitIndex);
                                    
                                    ValueEntry newV = new ValueEntry(bytes, DataType.STRING, -1);
                                    if (v != null) newV.expireAt = v.expireAt;
                                    newV.touch();
                                    return newV;
                                });
                                notifyWatchers(key);
                                aofHandler.log("SETBIT", key, String.valueOf(offset), String.valueOf(val));
                                send(out, isResp, Resp.integer(oldBit[0]), "(integer) " + oldBit[0]);
                            }
                        } catch (NumberFormatException e) {
                            send(out, isResp, Resp.error("ERR bit offset is not an integer or out of range"), "(error) ERR bit offset is not an integer or out of range");
                        } catch (RuntimeException e) {
                            String msg = e.getMessage();
                            if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                                send(out, isResp, Resp.error(msg), "(error) " + msg);
                            else throw e;
                        }
                    }
                    break;
                
                case "GETBIT":
                    if (parts.size() < 3) send(out, isResp, Resp.error("usage: GETBIT key offset"), "(error) usage: GETBIT key offset");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        try {
                            int offset = Integer.parseInt(new String(parts.get(2), StandardCharsets.UTF_8));
                            ValueEntry entry = store.get(key);
                            if (entry == null) {
                                send(out, isResp, Resp.integer(0), "(integer) 0");
                            } else if (entry.type != DataType.STRING) {
                                send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                            } else {
                                byte[] bytes = (byte[]) entry.value;
                                int byteIndex = offset / 8;
                                int bitIndex = 7 - (offset % 8);
                                
                                int bit = 0;
                                if (byteIndex < bytes.length) {
                                    bit = (bytes[byteIndex] >> bitIndex) & 1;
                                }
                                send(out, isResp, Resp.integer(bit), "(integer) " + bit);
                            }
                        } catch (NumberFormatException e) {
                            send(out, isResp, Resp.error("ERR bit offset is not an integer or out of range"), "(error) ERR bit offset is not an integer or out of range");
                        }
                    }
                    break;

                case "MSET":
                    if (parts.size() < 3 || (parts.size() - 1) % 2 != 0) {
                        send(out, isResp, Resp.error("wrong number of arguments for 'mset' command"), "(error) wrong number of arguments for 'mset' command");
                    } else {
                        performEvictionIfNeeded();
                        for (int i = 1; i < parts.size(); i += 2) {
                            String key = new String(parts.get(i), StandardCharsets.UTF_8);
                            byte[] val = parts.get(i + 1);
                            store.put(key, new ValueEntry(val, DataType.STRING, -1));
                            notifyWatchers(key);
                        }
                        if (aofHandler != null) {
                            Object[] logArgs = new Object[parts.size() - 1];
                            for(int i=1; i<parts.size(); i++) {
                                if ((i-1) % 2 == 0) logArgs[i-1] = new String(parts.get(i), StandardCharsets.UTF_8);
                                else logArgs[i-1] = parts.get(i);
                            }
                            aofHandler.log("MSET", logArgs);
                        }
                        send(out, isResp, Resp.simpleString("OK"), "OK");
                    }
                    break;

                case "GET":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: GET key"), "(error) usage: GET key");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                        if (entry == null) send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                        else if (entry.isExpired()) { store.remove(key); send(out, isResp, Resp.bulkString((byte[])null), "(nil)"); }
                        else { 
                            entry.touch(); // LRU update
                            if (entry.type != DataType.STRING && entry.type != null) {
                                send(out, isResp, Resp.error("WRONGTYPE Operation against a key holding the wrong kind of value"), "(error) WRONGTYPE");
                            } else {
                                byte[] v = (byte[]) entry.value;
                                send(out, isResp, Resp.bulkString(v), new String(v, StandardCharsets.UTF_8));
                            }
                        }
                    }
                    break;

                case "MGET":
                    if (parts.size() < 2) {
                        send(out, isResp, Resp.error("wrong number of arguments for 'mget' command"), "(error) wrong number of arguments for 'mget' command");
                    } else {
                        if (isResp) {
                            List<byte[]> results = new ArrayList<>();
                            for (int i = 1; i < parts.size(); i++) {
                                String key = new String(parts.get(i), StandardCharsets.UTF_8);
                                ValueEntry entry = store.get(key);
                                if (entry == null || entry.isExpired() || entry.type != DataType.STRING) {
                                    if (entry != null && entry.isExpired()) store.remove(key);
                                    results.add(null);
                                } else {
                                    entry.touch();
                                    results.add((byte[]) entry.value);
                                }
                            }
                            send(out, true, Resp.array(results), null);
                        } else {
                            // Text mode fallback
                            List<String> results = new ArrayList<>();
                            for (int i = 1; i < parts.size(); i++) {
                                String key = new String(parts.get(i), StandardCharsets.UTF_8);
                                ValueEntry entry = store.get(key);
                                if (entry == null || entry.isExpired() || entry.type != DataType.STRING) {
                                    if (entry != null && entry.isExpired()) store.remove(key);
                                    results.add(null);
                                } else {
                                    entry.touch();
                                    results.add(new String((byte[]) entry.value, StandardCharsets.UTF_8));
                                }
                            }
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
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
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
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        try {
                            long seconds = Long.parseLong(new String(parts.get(2), StandardCharsets.UTF_8));
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
                        String pattern = new String(parts.get(1), StandardCharsets.UTF_8);
                        List<byte[]> keys = new ArrayList<>();
                        List<String> keyStrings = new ArrayList<>();
                        if (pattern.equals("*")) {
                            for(String k : store.keySet()) {
                                keys.add(k.getBytes(StandardCharsets.UTF_8));
                                keyStrings.add(k);
                            }
                        } else {
                            String regex = pattern.replace(".", "\\.").replace("*", ".*").replace("?", ".");
                            java.util.regex.Pattern p = java.util.regex.Pattern.compile(regex);
                            for (String k : store.keySet()) {
                                if (p.matcher(k).matches()) {
                                    keys.add(k.getBytes(StandardCharsets.UTF_8));
                                    keyStrings.add(k);
                                }
                            }
                        }
                        if (isResp) send(out, true, Resp.array(keys), null);
                        else {
                            StringBuilder sb = new StringBuilder();
                            for (int i = 0; i < keyStrings.size(); i++) sb.append((i+1) + ") \"" + keyStrings.get(i) + "\"\n");
                            send(out, false, null, sb.toString().trim());
                        }
                    }
                    break;

                case "INCR":
                case "DECR":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: "+cmd+" key"), "(error) usage: "+cmd+" key");
                    else {
                        performEvictionIfNeeded();
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
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
                                        val = Long.parseLong(new String((byte[])v.value, StandardCharsets.UTF_8));
                                    } catch (NumberFormatException e) {
                                        throw new RuntimeException("ERR value is not an integer or out of range");
                                    }
                                }
                                
                                if (cmd.equals("INCR")) val++; else val--;
                                ret[0] = val;
                                
                                ValueEntry newV = new ValueEntry(String.valueOf(val).getBytes(StandardCharsets.UTF_8), DataType.STRING, -1);
                                if (v != null) newV.expireAt = v.expireAt;
                                newV.touch();
                                return newV;
                            });
                            notifyWatchers(key);
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
                    if (parts.size() < 3) send(out, isResp, Resp.error("usage: "+cmd+" key value [value ...]"), "(error) usage: "+cmd+" key value [value ...]");
                    else {
                        performEvictionIfNeeded();
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        try {
                            store.compute(key, (k, v) -> {
                                ConcurrentLinkedDeque<String> list;
                                if (v == null) {
                                    list = new ConcurrentLinkedDeque<>();
                                    v = new ValueEntry(list, DataType.LIST, -1);
                                } else if (v.type != DataType.LIST) {
                                    throw new RuntimeException("WRONGTYPE");
                                } else {
                                    list = (ConcurrentLinkedDeque<String>) v.value;
                                }
                                
                                for (int i = 2; i < parts.size(); i++) {
                                    String val = new String(parts.get(i), StandardCharsets.UTF_8);
                                    if (cmd.equals("LPUSH")) list.addFirst(val); else list.addLast(val);
                                }
                                
                                v.touch(); // Update LRU
                                return v;
                            });
                            
                            notifyWatchers(key);

                            // Log: cmd key v1 v2 ...
                            Object[] args = new Object[parts.size()-1];
                            for(int i=1; i<parts.size(); i++) args[i-1] = new String(parts.get(i), StandardCharsets.UTF_8);
                            aofHandler.log(cmd, args);
                            
                            checkBlockers(key); // Notify waiters
                            ValueEntry v = store.get(key);
                            int size = (v != null && v.value instanceof Deque) ? ((Deque)v.value).size() : 0;
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
                            double timeout = Double.parseDouble(new String(parts.get(parts.size()-1), StandardCharsets.UTF_8));
                            List<String> keys = new ArrayList<>();
                            for(int i=1; i<parts.size()-1; i++) keys.add(new String(parts.get(i), StandardCharsets.UTF_8));
                            
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
                                            notifyWatchers(k);
                                            List<byte[]> resp = new ArrayList<>();
                                            resp.add(k.getBytes(StandardCharsets.UTF_8));
                                            resp.add(val.getBytes(StandardCharsets.UTF_8));
                                            send(out, isResp, Resp.array(resp), null);
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
                                    List<byte[]> result;
                                    if (timeout <= 0) {
                                        result = bReq.future.get(); 
                                    } else {
                                        result = bReq.future.get((long)(timeout * 1000), TimeUnit.MILLISECONDS);
                                    }
                                    send(out, isResp, Resp.array(result), null);
                                } catch (TimeoutException e) {
                                    bReq.future.cancel(true);
                                    send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                                } catch (Exception e) {
                                    bReq.future.cancel(true);
                                    send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
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
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                        if (entry == null) send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                        else if (entry.type != DataType.LIST) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                        else {
                            ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) entry.value;
                            if (list.isEmpty()) send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                            else {
                                String val = cmd.equals("LPOP") ? list.pollFirst() : list.pollLast(); // poll is safe
                                if (val != null) {
                                    aofHandler.log(cmd, key);
                                    if (list.isEmpty()) store.remove(key); // Remove empty list
                                    notifyWatchers(key);
                                    send(out, isResp, Resp.bulkString(val.getBytes(StandardCharsets.UTF_8)), val);
                                } else {
                                    send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                                }
                            }
                        }
                    }
                    break;
                    
                case "LRANGE":
                    if (parts.size() < 4) send(out, isResp, Resp.error("usage: LRANGE key start stop"), "(error) usage: LRANGE key start stop");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                        if (entry == null) send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                        else if (entry.type != DataType.LIST) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                        else {
                            ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) entry.value;
                            int size = list.size(); // Approximate size
                            int start = Integer.parseInt(new String(parts.get(2), StandardCharsets.UTF_8));
                            int end = Integer.parseInt(new String(parts.get(3), StandardCharsets.UTF_8));
                            
                            if (start < 0) start += size;
                            if (end < 0) end += size;
                            if (start < 0) start = 0;
                            
                            List<byte[]> sub = new ArrayList<>();
                            List<String> subStr = new ArrayList<>();
                            if (start <= end) {
                                Iterator<String> it = list.iterator();
                                int idx = 0;
                                while (it.hasNext() && idx <= end) {
                                    String s = it.next();
                                    if (idx >= start) {
                                        sub.add(s.getBytes(StandardCharsets.UTF_8));
                                        subStr.add(s);
                                    }
                                    idx++;
                                }
                            }
                            if (isResp) send(out, true, Resp.array(sub), null);
                            else {
                                StringBuilder sb = new StringBuilder();
                                for (int i = 0; i < subStr.size(); i++) sb.append((i+1) + ") \"" + subStr.get(i) + "\"\n");
                                send(out, false, null, sb.toString().trim());
                            }
                        }
                    }
                    break;

                // --- HASHES ---
                case "HSET":
                    if (parts.size() < 4 || (parts.size() - 2) % 2 != 0) {
                        send(out, isResp, Resp.error("usage: HSET key field value [field value ...]"), "(error) usage: HSET key field value [field value ...]");
                    } else {
                        performEvictionIfNeeded();
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        final int[] ret = {0}; // Number of fields added
                        try {
                            store.compute(key, (k, v) -> {
                                ConcurrentHashMap<String, String> map;
                                if (v == null) {
                                    map = new ConcurrentHashMap<>();
                                    v = new ValueEntry(map, DataType.HASH, -1);
                                } else if (v.type != DataType.HASH) {
                                    throw new RuntimeException("WRONGTYPE");
                                } else {
                                    map = (ConcurrentHashMap<String, String>) v.value;
                                }
                                
                                for (int i = 2; i < parts.size(); i += 2) {
                                    String field = new String(parts.get(i), StandardCharsets.UTF_8);
                                    String val = new String(parts.get(i+1), StandardCharsets.UTF_8);
                                    if (map.put(field, val) == null) ret[0]++; 
                                }
                                
                                v.touch();
                                return v;
                            });
                            
                            notifyWatchers(key);
                            String[] args = new String[parts.size()-1];
                            for(int i=1; i<parts.size(); i++) args[i-1] = new String(parts.get(i), StandardCharsets.UTF_8);
                            aofHandler.log("HSET", args);
                            
                            send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                        } catch (RuntimeException e) {
                            send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                        }
                    }
                    break;
                    
                case "HGET":
                    if (parts.size() < 3) send(out, isResp, Resp.error("usage: HGET key field"), "(error) usage: HGET key field");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String field = new String(parts.get(2), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                        if (entry == null || entry.type != DataType.HASH) send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                        else {
                            ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) entry.value;
                            String val = map.get(field);
                            send(out, isResp, Resp.bulkString(val != null ? val.getBytes(StandardCharsets.UTF_8) : null), val != null ? val : "(nil)");
                        }
                    }
                    break;
                    
                case "HGETALL":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: HGETALL key"), "(error) usage: HGETALL key");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                        if (entry == null || entry.type != DataType.HASH) send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                        else {
                            ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) entry.value;
                            List<byte[]> flat = new ArrayList<>();
                            List<String> flatStr = new ArrayList<>();
                            for (Map.Entry<String, String> e : map.entrySet()) {
                                flat.add(e.getKey().getBytes(StandardCharsets.UTF_8));
                                flat.add(e.getValue().getBytes(StandardCharsets.UTF_8));
                                flatStr.add(e.getKey());
                                flatStr.add(e.getValue());
                            }
                            if (isResp) send(out, true, Resp.array(flat), null);
                            else {
                                StringBuilder sb2 = new StringBuilder();
                                for (int i = 0; i < flatStr.size(); i++) {
                                        sb2.append((i+1) + ") \"" + flatStr.get(i) + "\"\n");
                                }
                                send(out, false, null, sb2.toString().trim());
                            }
                        }
                    }
                    break;
                
                case "HDEL":
                    if (parts.size() < 3) send(out, isResp, Resp.error("usage: HDEL key field"), "(error) usage: HDEL key field");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String field = new String(parts.get(2), StandardCharsets.UTF_8);
                        final int[] ret = {0};
                        store.computeIfPresent(key, (k, v) -> {
                            if (v.type == DataType.HASH) {
                                ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.value;
                                if (map.remove(field) != null) ret[0] = 1;
                                if (map.isEmpty()) return null;
                            }
                            return v;
                        });
                        if (ret[0] == 1) {
                            notifyWatchers(key);
                            aofHandler.log("HDEL", key, field);
                        }
                        send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                    }
                    break;

                case "HINCRBY":
                    if (parts.size() < 4) send(out, isResp, Resp.error("usage: HINCRBY key field increment"), "(error) usage: HINCRBY key field increment");
                    else {
                        performEvictionIfNeeded();
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String field = new String(parts.get(2), StandardCharsets.UTF_8);
                        final long[] ret = {0};
                        try {
                            long incr = Long.parseLong(new String(parts.get(3), StandardCharsets.UTF_8));
                            store.compute(key, (k, v) -> {
                                if (v == null) {
                                    ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
                                    map.put(field, String.valueOf(incr));
                                    ret[0] = incr;
                                    return new ValueEntry(map, DataType.HASH, -1);
                                } else if (v.type != DataType.HASH) {
                                    throw new RuntimeException("WRONGTYPE");
                                } else {
                                    ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.value;
                                    map.compute(field, (f, val) -> {
                                        long oldVal = 0;
                                        if (val != null) {
                                            try { oldVal = Long.parseLong(val); } catch (Exception e) { throw new RuntimeException("ERR hash value is not an integer"); }
                                        }
                                        long newVal = oldVal + incr;
                                        ret[0] = newVal;
                                        return String.valueOf(newVal);
                                    });
                                    v.touch();
                                    return v;
                                }
                            });
                            notifyWatchers(key);
                            aofHandler.log("HINCRBY", key, field, new String(parts.get(3), StandardCharsets.UTF_8));
                            send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                        } catch (NumberFormatException e) {
                                send(out, isResp, Resp.error("ERR value is not an integer or out of range"), "(error) ERR value is not an integer or out of range");
                        } catch (RuntimeException e) {
                                String msg = e.getMessage();
                                if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                                    send(out, isResp, Resp.error(msg), "(error) " + msg);
                                else throw e;
                        }
                    }
                    break;

                // --- SETS ---
                case "SADD":
                    if (parts.size() < 3) send(out, isResp, Resp.error("usage: SADD key member"), "(error) usage: SADD key member");
                    else {
                        performEvictionIfNeeded();
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String member = new String(parts.get(2), StandardCharsets.UTF_8);
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
                            if (ret[0] == 1) notifyWatchers(key);
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
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                        if (entry == null || entry.type != DataType.SET) send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                        else {
                            Set<String> set = (Set<String>) entry.value;
                            List<byte[]> list = new ArrayList<>();
                            List<String> listStr = new ArrayList<>();
                            for(String s : set) {
                                list.add(s.getBytes(StandardCharsets.UTF_8));
                                listStr.add(s);
                            }
                            if (isResp) send(out, true, Resp.array(list), null);
                            else {
                                StringBuilder sb = new StringBuilder();
                                for (int i = 0; i < listStr.size(); i++) sb.append((i+1) + ") \"" + listStr.get(i) + "\"\n");
                                send(out, false, null, sb.toString().trim());
                            }
                        }
                    }
                    break;

                case "SREM":
                    if (parts.size() < 3) send(out, isResp, Resp.error("usage: SREM key member"), "(error) usage: SREM key member");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String member = new String(parts.get(2), StandardCharsets.UTF_8);
                        final int[] ret = {0};
                        store.computeIfPresent(key, (k, v) -> {
                            if (v.type == DataType.SET) {
                                Set<String> set = (Set<String>) v.value;
                                if (set.remove(member)) ret[0] = 1;
                                if (set.isEmpty()) return null;
                            }
                            return v;
                        });
                        if (ret[0] == 1) {
                             notifyWatchers(key);
                             aofHandler.log("SREM", key, member);
                        }
                        send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                    }
                    break;
                
                case "SISMEMBER":
                    if (parts.size() < 3) send(out, isResp, Resp.error("usage: SISMEMBER key member"), "(error) usage: SISMEMBER key member");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String member = new String(parts.get(2), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                        if (entry == null || entry.type != DataType.SET) send(out, isResp, Resp.integer(0), "(integer) 0");
                        else {
                            Set<String> set = (Set<String>) entry.value;
                            send(out, isResp, Resp.integer(set.contains(member) ? 1 : 0), "(integer) " + (set.contains(member) ? 1 : 0));
                        }
                    }
                    break;
                    
                case "SCARD":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: SCARD key"), "(error) usage: SCARD key");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                        if (entry == null || entry.type != DataType.SET) send(out, isResp, Resp.integer(0), "(integer) 0");
                        else {
                            Set<String> set = (Set<String>) entry.value;
                            send(out, isResp, Resp.integer(set.size()), "(integer) " + set.size());
                        }
                    }
                    break;
                    
                case "SINTER":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: SINTER key [key ...]"), "(error) usage: SINTER key [key ...]");
                    else {
                        String firstKey = new String(parts.get(1), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(firstKey);
                        if (entry == null || entry.type != DataType.SET) {
                             send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                        } else {
                             Set<String> result = new HashSet<>((Set<String>) entry.value);
                             for (int i = 2; i < parts.size(); i++) {
                                 ValueEntry e = store.get(new String(parts.get(i), StandardCharsets.UTF_8));
                                 if (e == null || e.type != DataType.SET) {
                                     result.clear();
                                     break;
                                 }
                                 result.retainAll((Set<String>) e.value);
                             }
                             
                             if (isResp) {
                                 List<byte[]> resp = new ArrayList<>();
                                 for(String s : result) resp.add(s.getBytes(StandardCharsets.UTF_8));
                                 send(out, true, Resp.array(resp), null);
                             }
                             else {
                                 StringBuilder sb = new StringBuilder();
                                 int i = 1;
                                 for (String s : result) sb.append(i++).append(") \"").append(s).append("\"\n");
                                 send(out, false, null, sb.toString().trim());
                             }
                        }
                    }
                    break;

                case "SUNION":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: SUNION key [key ...]"), "(error) usage: SUNION key [key ...]");
                    else {
                         Set<String> result = new HashSet<>();
                         for (int i = 1; i < parts.size(); i++) {
                             ValueEntry e = store.get(new String(parts.get(i), StandardCharsets.UTF_8));
                             if (e != null && e.type == DataType.SET) {
                                 result.addAll((Set<String>) e.value);
                             }
                         }
                         if (isResp) {
                             List<byte[]> resp = new ArrayList<>();
                             for(String s : result) resp.add(s.getBytes(StandardCharsets.UTF_8));
                             send(out, true, Resp.array(resp), null);
                         }
                         else {
                             StringBuilder sb = new StringBuilder();
                             int i = 1;
                             for (String s : result) sb.append(i++).append(") \"").append(s).append("\"\n");
                             send(out, false, null, sb.toString().trim());
                         }
                    }
                    break;

                case "SDIFF":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: SDIFF key [key ...]"), "(error) usage: SDIFF key [key ...]");
                    else {
                        String firstKey = new String(parts.get(1), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(firstKey);
                        Set<String> result = new HashSet<>();
                        if (entry != null && entry.type == DataType.SET) {
                            result.addAll((Set<String>) entry.value);
                        }
                        
                        for (int i = 2; i < parts.size(); i++) {
                             ValueEntry e = store.get(new String(parts.get(i), StandardCharsets.UTF_8));
                             if (e != null && e.type == DataType.SET) {
                                 result.removeAll((Set<String>) e.value);
                             }
                        }
                        
                        if (isResp) {
                             List<byte[]> resp = new ArrayList<>();
                             for(String s : result) resp.add(s.getBytes(StandardCharsets.UTF_8));
                             send(out, true, Resp.array(resp), null);
                        }
                        else {
                             StringBuilder sb = new StringBuilder();
                             int i = 1;
                             for (String s : result) sb.append(i++).append(") \"").append(s).append("\"\n");
                             send(out, false, null, sb.toString().trim());
                         }
                    }
                    break;

                case "DEL":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: DEL key"), "(error) usage: DEL key");
                    else { 
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        ValueEntry prev = store.remove(key);
                        if (prev != null) {
                            notifyWatchers(key);
                            aofHandler.log("DEL", key);
                        }
                        send(out, isResp, Resp.integer(prev != null ? 1 : 0), "(integer) " + (prev != null ? 1 : 0));
                    }
                    break;
                
                // --- SORTED SETS ---
                case "ZADD":
                    if (parts.size() < 4 || (parts.size() - 2) % 2 != 0) {
                            send(out, isResp, Resp.error("usage: ZADD key score member [score member ...]"), "(error) usage: ZADD key score member ...");
                    } else {
                            performEvictionIfNeeded();
                            String key = new String(parts.get(1), StandardCharsets.UTF_8);
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
                                            double score = Double.parseDouble(new String(parts.get(i), StandardCharsets.UTF_8));
                                            String member = new String(parts.get(i+1), StandardCharsets.UTF_8);
                                            addedCount[0] += zset.add(score, member);
                                        } catch (Exception ex) {}
                                    }
                                    v.touch();
                                    return v;
                                });
                                
                                notifyWatchers(key);
                                String[] args = new String[parts.size()-1];
                                for(int i=1; i<parts.size(); i++) args[i-1] = new String(parts.get(i), StandardCharsets.UTF_8);
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
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        boolean withScores = parts.size() > 4 && new String(parts.get(parts.size()-1), StandardCharsets.UTF_8).equalsIgnoreCase("WITHSCORES");
                        
                        ValueEntry entry = store.get(key);
                        if (entry == null || entry.type != DataType.ZSET) {
                            if (entry != null && entry.type != DataType.ZSET) {
                                send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                            } else {
                                send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                            }
                        } else {
                            try {
                                int start = Integer.parseInt(new String(parts.get(2), StandardCharsets.UTF_8));
                                int end = Integer.parseInt(new String(parts.get(3), StandardCharsets.UTF_8));
                                CaradeZSet zset = (CaradeZSet) entry.value;
                                int size = zset.size();
                                
                                if (start < 0) start += size;
                                if (end < 0) end += size;
                                if (start < 0) start = 0;
                                
                                List<byte[]> result = new ArrayList<>();
                                List<String> resultStr = new ArrayList<>();
                                if (start <= end) {
                                    Iterator<ZNode> it = zset.sorted.iterator();
                                    int idx = 0;
                                    while (it.hasNext() && idx <= end) {
                                        ZNode node = it.next();
                                        if (idx >= start) {
                                            result.add(node.member.getBytes(StandardCharsets.UTF_8));
                                            resultStr.add(node.member);
                                            if (withScores) {
                                                String s = String.valueOf(node.score);
                                                if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                                                result.add(s.getBytes(StandardCharsets.UTF_8));
                                                resultStr.add(s);
                                            }
                                        }
                                        idx++;
                                    }
                                }
                                
                                if (isResp) send(out, true, Resp.array(result), null);
                                else {
                                        StringBuilder sb = new StringBuilder();
                                        for (int i = 0; i < resultStr.size(); i++) {
                                            sb.append((i+1) + ") \"" + resultStr.get(i) + "\"\n");
                                        }
                                        send(out, false, null, sb.toString().trim());
                                }
                                
                            } catch (NumberFormatException e) {
                                send(out, isResp, Resp.error("ERR value is not an integer or out of range"), "(error) ERR value is not an integer or out of range");
                            }
                        }
                    }
                    break;
                    
                case "ZREVRANGE":
                    if (parts.size() < 4) send(out, isResp, Resp.error("usage: ZREVRANGE key start stop [WITHSCORES]"), "(error) usage: ZREVRANGE key start stop [WITHSCORES]");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        boolean withScores = parts.size() > 4 && new String(parts.get(parts.size()-1), StandardCharsets.UTF_8).equalsIgnoreCase("WITHSCORES");
                        
                        ValueEntry entry = store.get(key);
                        if (entry == null || entry.type != DataType.ZSET) {
                             if (entry != null && entry.type != DataType.ZSET) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                             else send(out, isResp, Resp.array(Collections.emptyList()), "(empty list or set)");
                        } else {
                            try {
                                int start = Integer.parseInt(new String(parts.get(2), StandardCharsets.UTF_8));
                                int end = Integer.parseInt(new String(parts.get(3), StandardCharsets.UTF_8));
                                CaradeZSet zset = (CaradeZSet) entry.value;
                                int size = zset.size();
                                
                                if (start < 0) start += size;
                                if (end < 0) end += size;
                                if (start < 0) start = 0;
                                
                                List<byte[]> result = new ArrayList<>();
                                List<String> resultStr = new ArrayList<>();
                                if (start <= end) {
                                    Iterator<ZNode> it = zset.sorted.descendingIterator();
                                    int idx = 0;
                                    while (it.hasNext() && idx <= end) {
                                        ZNode node = it.next();
                                        if (idx >= start) {
                                            result.add(node.member.getBytes(StandardCharsets.UTF_8));
                                            resultStr.add(node.member);
                                            if (withScores) {
                                                String s = String.valueOf(node.score);
                                                if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                                                result.add(s.getBytes(StandardCharsets.UTF_8));
                                                resultStr.add(s);
                                            }
                                        }
                                        idx++;
                                    }
                                }
                                
                                if (isResp) send(out, true, Resp.array(result), null);
                                else {
                                        StringBuilder sb = new StringBuilder();
                                        for (int i = 0; i < resultStr.size(); i++) {
                                            sb.append((i+1) + ") \"" + resultStr.get(i) + "\"\n");
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
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String member = new String(parts.get(2), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                        if (entry == null || entry.type != DataType.ZSET) {
                                if (entry != null) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                                else send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                        } else {
                            CaradeZSet zset = (CaradeZSet) entry.value;
                            Double score = zset.score(member);
                            if (score == null) {
                                send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
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
                
                case "ZREM":
                     if (parts.size() < 3) send(out, isResp, Resp.error("usage: ZREM key member"), "(error) usage: ZREM key member");
                     else {
                         String key = new String(parts.get(1), StandardCharsets.UTF_8);
                         String member = new String(parts.get(2), StandardCharsets.UTF_8);
                         final int[] ret = {0};
                         store.computeIfPresent(key, (k, v) -> {
                             if (v.type == DataType.ZSET) {
                                 CaradeZSet zset = (CaradeZSet) v.value;
                                 Double score = zset.scores.remove(member);
                                 if (score != null) {
                                     zset.sorted.remove(new ZNode(score, member));
                                     ret[0] = 1;
                                 }
                                 if (zset.scores.isEmpty()) return null;
                             }
                             return v;
                         });
                         if (ret[0] == 1) {
                             notifyWatchers(key);
                             aofHandler.log("ZREM", key, member);
                         }
                         send(out, isResp, Resp.integer(ret[0]), "(integer) " + ret[0]);
                     }
                     break;
                
                case "ZINCRBY":
                    if (parts.size() < 4) send(out, isResp, Resp.error("usage: ZINCRBY key increment member"), "(error) usage: ZINCRBY key increment member");
                    else {
                        performEvictionIfNeeded();
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String member = new String(parts.get(3), StandardCharsets.UTF_8);
                        final double[] ret = {0.0};
                        try {
                            double incr = Double.parseDouble(new String(parts.get(2), StandardCharsets.UTF_8));
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
                                ret[0] = zset.incrBy(incr, member);
                                v.touch();
                                return v;
                            });
                            notifyWatchers(key);
                            aofHandler.log("ZINCRBY", key, String.valueOf(incr), member);
                            
                            String s = String.valueOf(ret[0]);
                            if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                            send(out, isResp, Resp.bulkString(s.getBytes(StandardCharsets.UTF_8)), s);
                        } catch (NumberFormatException e) {
                            send(out, isResp, Resp.error("ERR value is not a valid float"), "(error) ERR value is not a valid float");
                        } catch (RuntimeException e) {
                            String msg = e.getMessage();
                            if (msg.startsWith("ERR") || msg.startsWith("WRONGTYPE"))
                                send(out, isResp, Resp.error(msg), "(error) " + msg);
                            else throw e;
                        }
                    }
                    break;

                case "ZCARD":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: ZCARD key"), "(error) usage: ZCARD key");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                        if (entry == null || entry.type != DataType.ZSET) {
                            if (entry != null && entry.type != DataType.ZSET) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                            else send(out, isResp, Resp.integer(0), "(integer) 0");
                        } else {
                            CaradeZSet zset = (CaradeZSet) entry.value;
                            send(out, isResp, Resp.integer(zset.size()), "(integer) " + zset.size());
                        }
                    }
                    break;

                case "ZCOUNT":
                    if (parts.size() < 4) send(out, isResp, Resp.error("usage: ZCOUNT key min max"), "(error) usage: ZCOUNT key min max");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                        if (entry == null || entry.type != DataType.ZSET) {
                             if (entry != null && entry.type != DataType.ZSET) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                             else send(out, isResp, Resp.integer(0), "(integer) 0");
                        } else {
                             try {
                                 String minStr = new String(parts.get(2), StandardCharsets.UTF_8).toLowerCase();
                                 String maxStr = new String(parts.get(3), StandardCharsets.UTF_8).toLowerCase();
                                 double min = minStr.equals("-inf") ? Double.NEGATIVE_INFINITY : (minStr.equals("+inf") || minStr.equals("inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(minStr));
                                 double max = maxStr.equals("-inf") ? Double.NEGATIVE_INFINITY : (maxStr.equals("+inf") || maxStr.equals("inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(maxStr));
                                 
                                 CaradeZSet zset = (CaradeZSet) entry.value;
                                 long count = 0;
                                 ZNode startNode = new ZNode(min, "");
                                 for (ZNode node : zset.sorted.tailSet(startNode)) {
                                     if (node.score > max) break;
                                     count++;
                                 }
                                 send(out, isResp, Resp.integer(count), "(integer) " + count);
                             } catch (NumberFormatException e) {
                                 send(out, isResp, Resp.error("ERR min or max is not a float"), "(error) ERR min or max is not a float");
                             }
                        }
                    }
                    break;

                case "ZSCORE":
                    if (parts.size() < 3) send(out, isResp, Resp.error("usage: ZSCORE key member"), "(error) usage: ZSCORE key member");
                    else {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String member = new String(parts.get(2), StandardCharsets.UTF_8);
                        ValueEntry entry = store.get(key);
                         if (entry == null || entry.type != DataType.ZSET) {
                             if (entry != null && entry.type != DataType.ZSET) send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
                             else send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                         } else {
                             CaradeZSet zset = (CaradeZSet) entry.value;
                             Double score = zset.score(member);
                             if (score == null) send(out, isResp, Resp.bulkString((byte[])null), "(nil)");
                             else {
                                 String s = String.valueOf(score);
                                 if (s.endsWith(".0")) s = s.substring(0, s.length()-2);
                                 send(out, isResp, Resp.bulkString(s.getBytes(StandardCharsets.UTF_8)), s);
                             }
                         }
                    }
                    break;

                // --- NEW PUB/SUB COMMANDS ---
                case "SUBSCRIBE":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: SUBSCRIBE channel"), "(error) usage: SUBSCRIBE channel");
                    else {
                        for (int i = 1; i < parts.size(); i++) {
                            String channel = new String(parts.get(i), StandardCharsets.UTF_8);
                            pubSub.subscribe(channel, this);
                            isSubscribed = true;
                            if (isResp) {
                                List<byte[]> resp = new ArrayList<>();
                                resp.add("subscribe".getBytes(StandardCharsets.UTF_8));
                                resp.add(channel.getBytes(StandardCharsets.UTF_8));
                                resp.add("1".getBytes(StandardCharsets.UTF_8));
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
                            if (isResp) {
                                List<byte[]> resp = new ArrayList<>();
                                resp.add("unsubscribe".getBytes(StandardCharsets.UTF_8));
                                resp.add(null);
                                resp.add("0".getBytes(StandardCharsets.UTF_8));
                                send(out, true, Resp.array(resp), null);
                            }
                            else send(out, false, null, "Unsubscribed from all");
                        }
                    } else {
                        for (int i = 1; i < parts.size(); i++) {
                            String channel = new String(parts.get(i), StandardCharsets.UTF_8);
                            pubSub.unsubscribe(channel, this);
                            if (isResp) {
                                List<byte[]> resp = new ArrayList<>();
                                resp.add("unsubscribe".getBytes(StandardCharsets.UTF_8));
                                resp.add(channel.getBytes(StandardCharsets.UTF_8));
                                resp.add("0".getBytes(StandardCharsets.UTF_8));
                                send(out, true, Resp.array(resp), null);
                            }
                            else send(out, false, null, "Unsubscribed from: " + channel);
                        }
                    }
                    break;

                case "PSUBSCRIBE":
                    if (parts.size() < 2) send(out, isResp, Resp.error("usage: PSUBSCRIBE pattern"), "(error) usage: PSUBSCRIBE pattern");
                    else {
                        for (int i = 1; i < parts.size(); i++) {
                            String pattern = new String(parts.get(i), StandardCharsets.UTF_8);
                            pubSub.psubscribe(pattern, this);
                            isSubscribed = true;
                            if (isResp) {
                                List<byte[]> resp = new ArrayList<>();
                                resp.add("psubscribe".getBytes(StandardCharsets.UTF_8));
                                resp.add(pattern.getBytes(StandardCharsets.UTF_8));
                                resp.add("1".getBytes(StandardCharsets.UTF_8));
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
                            String pattern = new String(parts.get(i), StandardCharsets.UTF_8);
                            pubSub.punsubscribe(pattern, this);
                            if (isResp) {
                                List<byte[]> resp = new ArrayList<>();
                                resp.add("punsubscribe".getBytes(StandardCharsets.UTF_8));
                                resp.add(pattern.getBytes(StandardCharsets.UTF_8));
                                resp.add("0".getBytes(StandardCharsets.UTF_8));
                                send(out, true, Resp.array(resp), null);
                            } 
                            else send(out, false, null, "Unsubscribed from pattern: " + pattern);
                            }
                    }
                    break;

                case "PUBLISH":
                    if (parts.size() < 3) send(out, isResp, Resp.error("usage: PUBLISH channel message"), "(error) usage: PUBLISH channel message");
                    else {
                        String channel = new String(parts.get(1), StandardCharsets.UTF_8);
                        String msg = new String(parts.get(2), StandardCharsets.UTF_8);
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
                    
                    if (isResp) send(out, true, Resp.bulkString(info.toString().getBytes(StandardCharsets.UTF_8)), null);
                    else send(out, false, null, info.toString());
                    break;
                case "DBSIZE": send(out, isResp, Resp.integer(store.size()), "(integer) " + store.size()); break;
                case "FLUSHALL": 
                    // Notify all watchers as all keys are gone
                    // We can't iterate all keys easily if map is huge, but we can iterate watchers map
                    // Or since store.clear() happens, we can just clear watchers map too?
                    // Actually, if we clear watchers map, we lose the clients references.
                    // We should iterate watchers and notify all of them.
                    for (String k : watchers.keySet()) {
                        notifyWatchers(k);
                    }
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
        }
    }
}