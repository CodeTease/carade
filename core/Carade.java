package core;

import core.db.CaradeDatabase;
import core.db.DataType;
import core.commands.CommandRegistry;
import core.commands.string.SetCommand;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.persistence.CommandLogger;
import core.persistence.rdb.RdbEncoder;
import core.persistence.rdb.RdbParser;
import core.structs.CaradeZSet;
import core.structs.ZNode;

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
    public static Config config;
    
    // --- METRICS ---
    public static final AtomicLong totalCommands = new AtomicLong(0);
    public static final AtomicInteger activeConnections = new AtomicInteger(0);

    // --- STORAGE ENGINE ---
    public static CaradeDatabase db;
    public static final ReentrantReadWriteLock globalRWLock = new ReentrantReadWriteLock();
    public static CommandLogger aofHandler;
    
    // --- SCAN ENGINE ---
    public static class ScanCursor {
        public final Iterator<?> iterator;
        public final DataType type;
        public volatile long lastAccess;
        public ScanCursor(Iterator<?> iterator, DataType type) {
            this.iterator = iterator;
            this.type = type;
            this.lastAccess = System.currentTimeMillis();
        }
    }
    public static final ConcurrentHashMap<String, ScanCursor> scanRegistry = new ConcurrentHashMap<>();
    public static final AtomicLong cursorIdGen = new AtomicLong(1);

    // --- WATCH / TRANSACTIONS ---
    public static final ConcurrentHashMap<String, Set<ClientHandler>> watchers = new ConcurrentHashMap<>();

    public static void notifyWatchers(String key) {
        Set<ClientHandler> interested = watchers.get(key);
        if (interested != null) {
            for (ClientHandler client : interested) {
                client.markDirty();
            }
        }
    }

    // --- BLOCKING QUEUES ---
    public static class BlockingRequest {
        public final CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        public final boolean isLeft;
        public BlockingRequest(boolean isLeft) { this.isLeft = isLeft; }
    }
    public static final ConcurrentHashMap<String, ConcurrentLinkedQueue<BlockingRequest>> blockingRegistry = new ConcurrentHashMap<>();

    public static void checkBlockers(String key) {
        ConcurrentLinkedQueue<BlockingRequest> q = blockingRegistry.get(key);
        if (q == null) return;
        
        synchronized (q) {
            while (!q.isEmpty()) {
                ValueEntry v = db.get(key);
                if (v == null || v.type != DataType.LIST) return;
                ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.getValue();
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
                    if (list.isEmpty()) db.remove(key);
                } else {
                    return;
                }
            }
        }
    }

    // --- PUB/SUB ENGINE (NEW!) ---
    public static PubSub pubSub = new PubSub();

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
        
        // Register Commands
        CommandRegistry.register("SET", new SetCommand());

        // Load Config
        config = Config.load("carade.conf");
        // Override with env var if present (legacy support)
        if (System.getenv("CARADE_PASSWORD") != null) {
            config.password = System.getenv("CARADE_PASSWORD");
            config.users.get("default").password = config.password;
        }

        // Initialize AOF
        aofHandler = CommandLogger.getInstance();
        
        // Initialize DB
        db = new CaradeDatabase(config, aofHandler);

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
                    System.out.println("[Janitor] Cleanup cycle completed. Database size: " + db.size());
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
                            activeConnections.get(), db.size(), pubSub.getChannelCount(), pubSub.getPatternCount(), ops);
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
    public static void performEvictionIfNeeded() {
        db.performEvictionIfNeeded();
    }

    // AOF Replay Logic
    // Internal AOF Replay State
    private static int replayDbIndex = 0;

    public static void executeInternal(List<byte[]> parts) {
        if (parts.isEmpty()) return;
        String cmd = new String(parts.get(0), StandardCharsets.UTF_8).toUpperCase();
        try {
            // Check for SELECT first
            if (cmd.equals("SELECT")) {
                 if (parts.size() >= 2) {
                     try {
                         replayDbIndex = Integer.parseInt(new String(parts.get(1), StandardCharsets.UTF_8));
                     } catch (Exception e) {}
                 }
                 return;
            }

            switch (cmd) {
                case "SET":
                    if (parts.size() >= 3) {
                        long ttl = -1;
                        if (parts.size() >= 5 && new String(parts.get(3), StandardCharsets.UTF_8).equalsIgnoreCase("EX")) {
                            try { ttl = Long.parseLong(new String(parts.get(4), StandardCharsets.UTF_8)); } catch (Exception e) {}
                        }
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        db.put(replayDbIndex, key, new ValueEntry(parts.get(2), DataType.STRING, ttl));
                    }
                    break;
                case "SETBIT":
                    if (parts.size() >= 4) {
                         String key = new String(parts.get(1), StandardCharsets.UTF_8);
                         try {
                             int offset = Integer.parseInt(new String(parts.get(2), StandardCharsets.UTF_8));
                             int val = Integer.parseInt(new String(parts.get(3), StandardCharsets.UTF_8));
                             db.getStore(replayDbIndex).compute(key, (k, v) -> {
                                 byte[] bytes;
                                 if (v == null) bytes = new byte[0];
                                 else if (v.type == DataType.STRING) bytes = (byte[]) v.getValue();
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
                         ValueEntry v = db.remove(replayDbIndex, oldKey);
                         if (v != null) db.put(replayDbIndex, newKey, v);
                    }
                    break;
                case "DEL":
                    if (parts.size() >= 2) db.remove(replayDbIndex, new String(parts.get(1), StandardCharsets.UTF_8));
                    break;
                case "HDEL":
                    if (parts.size() >= 3) {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String field = new String(parts.get(2), StandardCharsets.UTF_8);
                        db.getStore(replayDbIndex).computeIfPresent(key, (k, v) -> {
                            if (v.type == DataType.HASH) {
                                ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.getValue();
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
                         db.getStore(replayDbIndex).computeIfPresent(key, (k, v) -> {
                            if (v.type == DataType.SET) {
                                Set<String> set = (Set<String>) v.getValue();
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
                         db.getStore(replayDbIndex).computeIfPresent(key, (k, v) -> {
                            if (v.type == DataType.ZSET) {
                                CaradeZSet zset = (CaradeZSet) v.getValue();
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
                             db.getStore(replayDbIndex).compute(key, (k, v) -> {
                                 CaradeZSet zset;
                                 if (v == null) {
                                     zset = new CaradeZSet();
                                     v = new ValueEntry(zset, DataType.ZSET, -1);
                                 } else if (v.type == DataType.ZSET) {
                                     zset = (CaradeZSet)v.getValue();
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
                             db.getStore(replayDbIndex).compute(key, (k, v) -> {
                                 CaradeZSet zset;
                                 if (v == null) {
                                     zset = new CaradeZSet();
                                     v = new ValueEntry(zset, DataType.ZSET, -1);
                                 } else if (v.type == DataType.ZSET) {
                                     zset = (CaradeZSet)v.getValue();
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
                                db.put(replayDbIndex, key, new ValueEntry(val, DataType.STRING, -1));
                            }
                        }
                    }
                    break;
                case "LPUSH":
                case "RPUSH":
                    if (parts.size() >= 3) {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        String val = new String(parts.get(2), StandardCharsets.UTF_8);
                        db.getStore(replayDbIndex).compute(key, (k, v) -> {
                            if (v == null) {
                                ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>();
                                list.add(val);
                                return new ValueEntry(list, DataType.LIST, -1);
                            } else if (v.type == DataType.LIST) {
                                ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.getValue();
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
                        db.getStore(replayDbIndex).computeIfPresent(key, (k, v) -> {
                            if (v.type == DataType.LIST) {
                                ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.getValue();
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
                        db.getStore(replayDbIndex).compute(key, (k, v) -> {
                            if (v == null) {
                                ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
                                map.put(field, val);
                                return new ValueEntry(map, DataType.HASH, -1);
                            } else if (v.type == DataType.HASH) {
                                ((ConcurrentHashMap<String, String>) v.getValue()).put(field, val);
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
                             db.getStore(replayDbIndex).compute(key, (k, v) -> {
                                if (v == null) {
                                    ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
                                    map.put(field, String.valueOf(incr));
                                    return new ValueEntry(map, DataType.HASH, -1);
                                } else if (v.type != DataType.HASH) {
                                    return v;
                                } else {
                                    ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) v.getValue();
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
                         db.getStore(replayDbIndex).compute(key, (k, v) -> {
                            if (v == null) {
                                Set<String> set = ConcurrentHashMap.newKeySet();
                                set.add(member);
                                return new ValueEntry(set, DataType.SET, -1);
                            } else if (v.type == DataType.SET) {
                                ((Set<String>) v.getValue()).add(member);
                            }
                            return v;
                        });
                    }
                    break;
                case "FLUSHALL":
                    db.clearAll();
                    break;
                case "FLUSHDB":
                    db.clear(replayDbIndex);
                    break;
                case "INCR":
                case "DECR":
                    if (parts.size() >= 2) {
                        String key = new String(parts.get(1), StandardCharsets.UTF_8);
                        db.getStore(replayDbIndex).compute(key, (k, v) -> {
                            long val = 0;
                            if (v == null) {
                                val = 0;
                            } else if (v.type == DataType.STRING) {
                                try {
                                    val = Long.parseLong(new String((byte[])v.getValue(), java.nio.charset.StandardCharsets.UTF_8));
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
                             db.getStore(replayDbIndex).computeIfPresent(key, (k, v) -> {
                                 v.expireAt = System.currentTimeMillis() + (seconds * 1000);
                                 return v;
                             });
                         } catch (Exception e) {}
                    }
                    break;
                case "RPOPLPUSH":
                    if (parts.size() >= 3) {
                        String source = new String(parts.get(1), StandardCharsets.UTF_8);
                        String dest = new String(parts.get(2), StandardCharsets.UTF_8);
                        ValueEntry srcEntry = db.get(replayDbIndex, source);
                        if (srcEntry != null && srcEntry.type == DataType.LIST) {
                            ConcurrentLinkedDeque<String> srcList = (ConcurrentLinkedDeque<String>) srcEntry.getValue();
                            if (!srcList.isEmpty()) {
                                String val = srcList.pollLast();
                                if (srcList.isEmpty()) db.remove(replayDbIndex, source);
                                
                                db.getStore(replayDbIndex).compute(dest, (k, v) -> {
                                    if (v == null) {
                                        ConcurrentLinkedDeque<String> list = new ConcurrentLinkedDeque<>();
                                        list.addFirst(val);
                                        return new ValueEntry(list, DataType.LIST, -1);
                                    } else if (v.type == DataType.LIST) {
                                        ((ConcurrentLinkedDeque<String>) v.getValue()).addFirst(val);
                                        return v;
                                    }
                                    return v;
                                });
                            }
                        }
                    }
                    break;
            }
        } catch (Exception e) {
            System.err.println("⚠️ Error executing internal command: " + cmd + " - " + e.getMessage());
        }
    }
    
    private static void cleanupExpiredKeys() {
        int removed = 0;
        for (int i=0; i < CaradeDatabase.DB_COUNT; i++) {
             Iterator<Map.Entry<String, ValueEntry>> it = db.entrySet(i).iterator();
             while (it.hasNext()) {
                Map.Entry<String, ValueEntry> entry = it.next();
                if (entry.getValue().isExpired()) {
                    it.remove();
                    if (aofHandler != null) {
                        // TODO: Log SELECT if needed?
                        // For now we assume AOF logic handles context or we just log DEL
                        aofHandler.log("DEL", entry.getKey()); 
                    }
                    removed++;
                }
            }
        }
        if (removed > 0) System.out.println("🧹 [GC] Vacuumed " + removed + " expired keys.");
    }
    
    private static void cleanupExpiredCursors() {
        long now = System.currentTimeMillis();
        scanRegistry.entrySet().removeIf(e -> (now - e.getValue().lastAccess) > 600000); // 10 mins
    }

    private static void saveData() {
        try {
            new RdbEncoder().save(db, DUMP_FILE);
            System.out.println("💾 Snapshot saved (RDB).");
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
        
        try (InputStream is = new BufferedInputStream(new FileInputStream(f))) {
            is.mark(5);
            byte[] magic = new byte[5];
            is.read(magic);
            is.reset();
            
            String header = new String(magic);

            if (header.equals("REDIS")) {
                 System.out.println("📂 Detected RDB file. Loading...");
                 new RdbParser(is).parse(db);
                 System.out.println("📂 Loaded " + db.size() + " keys (RDB).");
            } else if (header.startsWith("CARD")) {
                 // Fallback to legacy CARD parser
                 try (DataInputStream dis = new DataInputStream(is)) { // Wrap the same stream
                     byte[] m = new byte[4];
                     dis.readFully(m); // Eat CARD
                     int version = dis.readInt();
                     if (version != 1) throw new IOException("Unknown version: " + version);
                     
                     db.clear();
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
                                value = s.getBytes(StandardCharsets.UTF_8);
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
                            if (!ve.isExpired()) db.put(key, ve);
                        } catch (EOFException e) { break; }
                     }
                     System.out.println("📂 Loaded " + db.size() + " keys (Snapshot CARD).");
                 }
            } else {
                 loadLegacyData(); // Object stream
            }
        } catch (Exception e) {
             System.out.println("⚠️ Load failed: " + e.getMessage());
             e.printStackTrace();
        }
    }
    
    private static void loadLegacyData() {
         File f = new File(DUMP_FILE);
         try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f))) {
            Object loaded = ois.readObject();
            if (loaded instanceof ConcurrentHashMap) {
                ConcurrentHashMap<?, ?> rawMap = (ConcurrentHashMap<?, ?>) loaded;
                db.clear();
                for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                    String key = (String) entry.getKey();
                    Object val = entry.getValue();
                    if (val instanceof String) {
                        db.put(key, new ValueEntry(((String) val).getBytes(java.nio.charset.StandardCharsets.UTF_8), DataType.STRING, -1));
                    } else if (val instanceof ValueEntry) {
                        ValueEntry ve = (ValueEntry) val;
                        if (ve.type == DataType.STRING && ve.getValue() instanceof String) {
                            ve.setValue(((String) ve.getValue()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        }
                        db.put(key, ve);
                    }
                }
                System.out.println("📂 Loaded " + db.size() + " keys (Legacy).");
                saveData();
            }
        } catch (Exception ex) {
            System.out.println("⚠️ Legacy load failed. Starting fresh.");
            db.clear();
        }
    }
}
