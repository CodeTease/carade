package core;

import core.db.CaradeDatabase;
import core.db.DataType;
import core.commands.Command;
import core.commands.CommandRegistry;
import core.commands.string.SetCommand;
import core.commands.string.GetCommand;
import core.commands.generic.DelCommand;
import core.commands.list.LPopCommand;
import core.commands.list.RPopCommand;
import core.commands.hash.HSetCommand;
import core.commands.hash.HGetCommand;
import core.commands.scripting.EvalRoCommand;
import core.commands.scripting.EvalShaRoCommand;
import core.db.ValueEntry;
import core.network.ClientHandler;
import core.persistence.CommandLogger;
import core.persistence.rdb.RdbEncoder;
import core.persistence.rdb.RdbParser;
import core.structs.CaradeZSet;
import core.structs.ZNode;
import core.protocol.netty.NettyRespDecoder;
import core.protocol.netty.NettyRespEncoder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.nio.charset.StandardCharsets;

import core.utils.Log;

/**
 * Project: Carade
 * Version: 0.3.0
 * Status: Stable idk / Probably production / Chaos / Forever
 * Author: CodeTease 
 */
public class Carade {

    // --- CONFIGURATION ---
    private static final String DUMP_FILE = "carade.dump";
    private static final String AOF_FILE = "carade.aof";
    public static Config config;
    
    // --- METRICS ---
    public static final AtomicLong totalCommands = new AtomicLong(0);
    public static final AtomicInteger activeConnections = new AtomicInteger(0);
    public static final Set<ClientHandler> connectedClients = ConcurrentHashMap.newKeySet();
    public static final AtomicLong keyspaceHits = new AtomicLong(0);
    public static final AtomicLong keyspaceMisses = new AtomicLong(0);
    
    // --- SLOWLOG ---
    public static final ConcurrentLinkedDeque<String> slowLog = new ConcurrentLinkedDeque<>();
    public static final int SLOWLOG_MAX_LEN = 128;

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

    // --- MONITOR ---
    public static final Set<ClientHandler> monitors = ConcurrentHashMap.newKeySet();

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
        public final String targetKey; // For BRPOPLPUSH
        public final int dbIndex;
        public final DataType expectedType;
        
        public BlockingRequest(boolean isLeft, int dbIndex) { this(isLeft, null, dbIndex, DataType.LIST); }
        public BlockingRequest(boolean isLeft, String targetKey, int dbIndex) { this(isLeft, targetKey, dbIndex, DataType.LIST); }
        public BlockingRequest(boolean isLeft, int dbIndex, DataType type) { this(isLeft, null, dbIndex, type); }
        
        public BlockingRequest(boolean isLeft, String targetKey, int dbIndex, DataType type) { 
            this.isLeft = isLeft; 
            this.targetKey = targetKey;
            this.dbIndex = dbIndex;
            this.expectedType = type;
        }
    }
    public static final ConcurrentHashMap<String, ConcurrentLinkedQueue<BlockingRequest>> blockingRegistry = new ConcurrentHashMap<>();

    public static void checkBlockers(String key) {
        ConcurrentLinkedQueue<BlockingRequest> q = blockingRegistry.get(key);
        if (q == null) return;
        
        synchronized (q) {
            while (!q.isEmpty()) {
                ValueEntry v = db.get(key);
                if (v == null) return;
                
                BlockingRequest req = q.peek();
                if (req.future.isDone()) {
                    q.poll(); 
                    continue;
                }
                
                if (v.type != req.expectedType) return;
                
                if (v.type == DataType.LIST) {
                    ConcurrentLinkedDeque<String> list = (ConcurrentLinkedDeque<String>) v.getValue();
                    if (list.isEmpty()) return;
                    
                    String val = req.isLeft ? list.pollFirst() : list.pollLast();
                    
                    if (val != null) {
                        q.poll();
                        
                        if (req.targetKey != null) {
                             // BRPOPLPUSH logic: push to target
                             db.getStore(req.dbIndex).compute(req.targetKey, (k, valEntry) -> {
                                 if (valEntry == null) {
                                     ConcurrentLinkedDeque<String> l = new ConcurrentLinkedDeque<>();
                                     l.addFirst(val);
                                     return new ValueEntry(l, DataType.LIST, -1);
                                 } else if (valEntry.type == DataType.LIST) {
                                     ((ConcurrentLinkedDeque<String>) valEntry.getValue()).addFirst(val);
                                     return valEntry;
                                 }
                                 return valEntry;
                             });
                             notifyWatchers(req.targetKey);
                        }
                        
                        boolean completed = req.future.complete(Arrays.asList(key.getBytes(StandardCharsets.UTF_8), val.getBytes(StandardCharsets.UTF_8)));
                        if (!completed) {
                            if (req.isLeft) list.addFirst(val); else list.addLast(val); 
                            if (req.targetKey != null) {
                                 db.getStore(req.dbIndex).computeIfPresent(req.targetKey, (k, valEntry) -> {
                                     if (valEntry.type == DataType.LIST) {
                                         ((ConcurrentLinkedDeque<String>) valEntry.getValue()).pollFirst();
                                     }
                                     return valEntry;
                                 });
                            }
                            continue;
                        }
                        
                        if (aofHandler != null) {
                            if (req.targetKey != null) {
                                aofHandler.log("RPOPLPUSH", key, req.targetKey);
                            } else {
                                aofHandler.log(req.isLeft ? "LPOP" : "RPOP", key);
                            }
                        }
                        if (list.isEmpty()) db.remove(key);
                    }
                } else if (v.type == DataType.ZSET) {
                    CaradeZSet zset = (CaradeZSet) v.getValue();
                    if (zset.size() == 0) return;
                    
                    // isLeft=true -> Min, isLeft=false -> Max
                    List<ZNode> nodes = req.isLeft ? zset.popMin(1) : zset.popMax(1);
                    if (nodes.isEmpty()) return;
                    ZNode node = nodes.get(0);
                    
                    q.poll();
                    
                    boolean completed = req.future.complete(Arrays.asList(
                        key.getBytes(StandardCharsets.UTF_8), 
                        node.member.getBytes(StandardCharsets.UTF_8), 
                        String.valueOf(node.score).getBytes(StandardCharsets.UTF_8)
                    ));
                    
                    if (!completed) {
                        zset.add(node.score, node.member); // Revert
                        continue;
                    }
                    
                    // Replicate/AOF as ZREM? Or assume ZPOP supported
                    // ZPOP is not standard for AOF in old redis, usually ZREM.
                    // But if we support ZPOP commands, we can log ZPOP.
                    // However, we popped.
                    if (aofHandler != null) {
                        aofHandler.log(req.isLeft ? "ZPOPMIN" : "ZPOPMAX", key);
                    }
                    if (zset.size() == 0) db.remove(key);
                }
            }
        }
    }

    // --- PUB/SUB ENGINE (NEW!) ---
    public static PubSub pubSub = new PubSub();

    public static volatile long pauseEndTime = 0; // For CLIENT PAUSE

    private static volatile boolean isRunning = true;

    public static void printBanner() {
        Log.info("\n" +
                "   ______                     __   \n" +
                "  / ____/___ ______________  / /__ \n" +
                " / /   / __ `/ ___/ __  / __  / _ \\\n" +
                "/ /___/ /_/ / /  / /_/ / /_/ /  __/\n" +
                "\\____/\\__,_/_/   \\__,_/\\__,_/\\___/ \n" +
                "                                   \n" +
                " :: Carade ::       (v0.3.0) \n" +
                " :: Engine ::       Java \n" +
                " :: Author ::       CodeTease \n");
    }

    private static final ClientHandler aofClient = new ClientHandler();

    public static void main(String[] args) throws Exception {
        Log.info("\n--- CARADE v0.3.0 ---\n");
        
        // Register Commands
        CommandRegistry.register("SET", new SetCommand());
        CommandRegistry.register("GET", new GetCommand());
        CommandRegistry.register("DEL", new DelCommand());
        CommandRegistry.register("LPOP", new LPopCommand());
        CommandRegistry.register("RPOP", new RPopCommand());
        CommandRegistry.register("HSET", new HSetCommand());
        CommandRegistry.register("HGET", new HGetCommand());
        CommandRegistry.register("EVAL_RO", new EvalRoCommand());
        CommandRegistry.register("EVALSHA_RO", new EvalShaRoCommand());

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
        aofHandler.replay(cmd -> executeAofCommand(cmd));

        // 1. Janitor (Refactored to ScheduledExecutorService)
        ScheduledExecutorService janitor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Janitor");
            t.setDaemon(true);
            return t;
        });

        // Use an array to hold state (lastMaintenance, currentDbIndex) because lambdas require final/effectively final
        // State: [0] = lastMaintenance, [1] = currentDbIndex
        final long[] janitorState = { System.currentTimeMillis(), 0 };
        final Iterator<Map.Entry<String, ValueEntry>>[] iterators = new Iterator[CaradeDatabase.DB_COUNT];

        janitor.scheduleAtFixedRate(() -> {
            try {
                // --- Active Expiration (Fast Loop ~100ms) ---
                if (db.size() > 0) {
                    int loopCount = 0;
                    int currentDbIndex = (int) janitorState[1];

                    // Adaptive loop: if too many keys expire, keep cleaning (limited by loopCount)
                    while (loopCount < 10) {
                        int keysToCheck = 20;
                        int expiredCount = 0;

                        if (iterators[currentDbIndex] == null || !iterators[currentDbIndex].hasNext()) {
                            iterators[currentDbIndex] = db.entrySet(currentDbIndex).iterator();
                        }

                        Iterator<Map.Entry<String, ValueEntry>> it = iterators[currentDbIndex];
                        while (it.hasNext() && keysToCheck > 0) {
                            Map.Entry<String, ValueEntry> entry = it.next();
                            if (entry.getValue().isExpired()) {
                                it.remove();
                                if (aofHandler != null) {
                                    aofHandler.log("SELECT", String.valueOf(currentDbIndex));
                                    aofHandler.log("DEL", entry.getKey());
                                }
                                expiredCount++;
                            }
                            keysToCheck--;
                        }
                        
                        // If we didn't find many expired keys, stop early
                        // 5 is 25% of 20
                        if (expiredCount <= 5) {
                            break;
                        }
                        
                        loopCount++;
                    }

                    // Update DB index for next run
                    janitorState[1] = (currentDbIndex + 1) % CaradeDatabase.DB_COUNT;
                }

                // --- Maintenance (Slow Loop ~30s) ---
                long now = System.currentTimeMillis();
                if (now - janitorState[0] > 30000) {
                    cleanupExpiredCursors();
                    saveData();
                    Log.info("[Janitor] Cleanup cycle completed. Database size: " + db.size());
                    janitorState[0] = now;
                }
            } catch (Exception e) {
                Log.error("[Janitor] Error: " + e.getMessage());
            }
        }, 0, 100, TimeUnit.MILLISECONDS);

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
                        Log.info(String.format("📊 [STATS] Clients: %d | Keys: %d | Channels: %d | Patterns: %d | OPS: %d cmd/s", 
                            activeConnections.get(), db.size(), pubSub.getChannelCount(), pubSub.getPatternCount(), ops));
                    }
                } catch (InterruptedException e) { break; }
            }
        });
        monitor.setDaemon(true);
        monitor.start();

        printBanner();

        // 3. Start Netty Server
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
             .channel(NioServerSocketChannel.class)
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 public void initChannel(SocketChannel ch) throws Exception {
                     ch.pipeline().addLast(new NettyRespDecoder());
                     ch.pipeline().addLast(new NettyRespEncoder());
                     ch.pipeline().addLast(new ClientHandler());
                 }
             });

            ChannelFuture f = b.bind(config.port).sync();
            Log.info("🔥 Ready on port " + config.port);
            Log.info("🔒 Max Memory: " + (config.maxMemory == 0 ? "Unlimited" : config.maxMemory + " bytes"));
            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                Log.info("\n🛑 Shutting down...");
                saveData();
                aofHandler.close();
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }));

            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    // --- LOGIC HELPER ---
    public static void performEvictionIfNeeded() {
        db.performEvictionIfNeeded();
    }

    // AOF Replay Logic
    public static void executeAofCommand(List<byte[]> parts) {
        if (parts.isEmpty()) return;
        String cmdName = new String(parts.get(0), StandardCharsets.UTF_8).toUpperCase();
        try {
            // Note: aofClient maintains dbIndex state across calls
            Command cmd = CommandRegistry.get(cmdName);
            if (cmd != null) {
                // AOF Replay happens before server starts listening, so concurrency is less of an issue,
                // but we should still respect the lock to be safe and consistent with Command logic.
                // Most commands don't lock internally, they expect caller to lock.
                // During startup replay, we are single-threaded.
                // But some commands might touch synchronized structures.
                cmd.execute(aofClient, parts);
            } else {
                 Log.error("⚠️ Unknown command in AOF: " + cmdName);
            }
        } catch (Exception e) {
            Log.error("⚠️ Error executing AOF command: " + cmdName + " - " + e.getMessage());
        }
    }
    
    private static void cleanupExpiredCursors() {
        long now = System.currentTimeMillis();
        scanRegistry.entrySet().removeIf(e -> (now - e.getValue().lastAccess) > 600000); // 10 mins
    }

    public static volatile long lastSaveTime = System.currentTimeMillis() / 1000;
    public static final AtomicBoolean isSaving = new AtomicBoolean(false);

    public static void saveData() {
        if (!isSaving.compareAndSet(false, true)) {
             throw new RuntimeException("Background save already in progress");
        }
        try {
            new RdbEncoder().save(db, DUMP_FILE);
            lastSaveTime = System.currentTimeMillis() / 1000;
            Log.info("💾 Snapshot saved (RDB).");
        } catch (IOException e) {
            Log.error("⚠️ Save failed: " + e.getMessage());
        } finally {
            isSaving.set(false);
        }
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
                 Log.info("📂 Detected RDB file. Loading...");
                 new RdbParser(is).parse(db);
                 Log.info("📂 Loaded " + db.size() + " keys (RDB).");
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
                     Log.info("📂 Loaded " + db.size() + " keys (Snapshot CARD).");
                 }
            }
        } catch (Exception e) {
             Log.error("⚠️ Load failed: " + e.getMessage());
             e.printStackTrace();
        }
    }
}
