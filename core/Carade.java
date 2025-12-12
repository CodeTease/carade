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
    enum DataType { STRING, LIST, HASH, SET }

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
    // Eviction Policy (Approximate LRU)
    private static void performEvictionIfNeeded() {
        if (config.maxMemory <= 0) return;
        
        // Simple heuristic: if we are roughly over memory.
        // Since we don't track exact memory usage of objects (hard in Java), 
        // we can rely on heap usage or estimated size. 
        // "Currently Carade will run until... your RAM cries".
        // The user wants "Max Memory... LRU".
        // Accurately tracking size of Java objects is complex. 
        // Let's assume the user configures maxMemory based on Heap size or we just check Runtime free memory?
        // Or we count keys? No, maxMemory implies bytes.
        // Let's use Runtime.getRuntime().totalMemory() - freeMemory() as approximation?
        // No, that includes overhead.
        
        // Strategy: Check if Runtime used memory > config.maxMemory.
        // If so, evict.
        
        long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (used < config.maxMemory) return;
        
        System.out.println("🧹 [Eviction] Memory full (" + (used/1024/1024) + "MB > " + (config.maxMemory/1024/1024) + "MB). Evicting...");
        
        // Evict loop: try to free up some space.
        // Sample random keys and remove LRU.
        int attempts = 0;
        while (used > config.maxMemory && !store.isEmpty() && attempts < 100) {
            // Sampling
            List<String> keys = new ArrayList<>(store.keySet()); // Expensive copy? keyset view is cheap but toArray is not.
            // Using iterator to get a few keys?
            // CHM iterator is weakly consistent.
            // Let's pick 5 random keys from a partial iteration.
            
            String bestKey = null;
            long oldestTime = Long.MAX_VALUE;
            
            Iterator<String> it = store.keySet().iterator();
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
            
            if (bestKey != null) {
                store.remove(bestKey);
                // Log eviction?
                // if (aofHandler != null) aofHandler.log("DEL", bestKey); // Maybe? Redis doesn't AOF eviction usually unless it's a delete.
                // It treats it as cache miss later. But if we want consistent state, we should.
                // But if we restart, memory is empty, so we reload. If we reload full AOF, we hit memory limit again and evict again.
                // So logging DEL is good.
                if (aofHandler != null) aofHandler.log("DEL", bestKey);
            }
            
            used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            attempts++;
            // Trigger GC? No, too slow. Just trust JVM will reclaim eventually. 
            // We just remove references.
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

    private static void saveData() {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(DUMP_FILE))) {
            oos.writeObject(store);
        } catch (IOException e) {
            System.err.println("⚠️ Save failed: " + e.getMessage());
        }
    }

    private static void loadData() {
        File f = new File(DUMP_FILE);
        if (!f.exists()) return;
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f))) {
            Object loaded = ois.readObject();
            if (loaded instanceof ConcurrentHashMap) {
                ConcurrentHashMap<?, ?> rawMap = (ConcurrentHashMap<?, ?>) loaded;
                store = new ConcurrentHashMap<>();
                int migratedCount = 0;
                for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                    String key = (String) entry.getKey();
                    Object val = entry.getValue();
                    if (val instanceof String) {
                        store.put(key, new ValueEntry((String) val, DataType.STRING, -1));
                        migratedCount++;
                    } else if (val instanceof ValueEntry) {
                        store.put(key, (ValueEntry) val);
                    }
                }
                System.out.println("📂 Loaded " + store.size() + " keys.");
                if (migratedCount > 0) saveData();
            }
        } catch (Exception e) {
            System.out.println("⚠️ Dump file incompatible. Creating new universe.");
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
                                        ValueEntry v = store.get(key);
                                        int size = ((List)v.value).size();
                                        send(out, isResp, Resp.integer(size), "(integer) " + size);
                                    } catch (RuntimeException e) {
                                        send(out, isResp, Resp.error("WRONGTYPE"), "(error) WRONGTYPE");
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
                                        // Snapshot to array for index access (inefficient but safe-ish)
                                        Object[] arr = list.toArray();
                                        int start = Integer.parseInt(parts.get(2));
                                        int end = Integer.parseInt(parts.get(3));
                                        if (start < 0) start += arr.length;
                                        if (end < 0) end += arr.length;
                                        if (start < 0) start = 0;
                                        if (end >= arr.length) end = arr.length - 1;
                                        
                                        List<String> sub = new ArrayList<>();
                                        if (start <= end) {
                                            for (int i = start; i <= end; i++) sub.add((String)arr[i]);
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

                            case "DBSIZE": send(out, isResp, Resp.integer(store.size()), "(integer) " + store.size()); break;
                            case "FLUSHALL": 
                                store.clear(); 
                                aofHandler.log("FLUSHALL");
                                send(out, isResp, Resp.simpleString("OK"), "OK"); 
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