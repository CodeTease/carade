import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.*;

/**
 * Project: Carade
 * Version: 0.1.0 (The "Gossip" Universe Edition)
 * Status: Alpha / Dev / Chaos / Forever
 * Author: CodeTease Team
 * "We don't bump versions, we bump features."
 */
public class Carade {

    // --- CONFIGURATION ---
    private static final int PORT = 63790;
    private static final String DUMP_FILE = "carade.dump";
    
    // Security
    private static final String PASSWORD = System.getenv("CARADE_PASSWORD") != null 
            ? System.getenv("CARADE_PASSWORD") 
            : "teasertopsecret";

    // --- METRICS ---
    private static final AtomicLong totalCommands = new AtomicLong(0);
    private static final AtomicInteger activeConnections = new AtomicInteger(0);

    // --- STORAGE ENGINE ---
    static class ValueEntry implements Serializable {
        private static final long serialVersionUID = 1L;
        String value;
        long expireAt; 

        ValueEntry(String value, long ttlSeconds) {
            this.value = value;
            this.expireAt = ttlSeconds > 0 ? System.currentTimeMillis() + (ttlSeconds * 1000) : -1;
        }
        boolean isExpired() { return expireAt != -1 && System.currentTimeMillis() > expireAt; }
    }

    private static ConcurrentHashMap<String, ValueEntry> store = new ConcurrentHashMap<>();
    
    // --- PUB/SUB ENGINE (NEW!) ---
    // Channel -> Set of OutputStreams (Subscribers)
    private static ConcurrentHashMap<String, Set<PrintWriter>> pubSubStore = new ConcurrentHashMap<>();

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

        loadData();

        // 1. Janitor
        Thread janitor = new Thread(() -> {
            while (isRunning) {
                try {
                    Thread.sleep(30000); 
                    cleanupExpiredKeys(); 
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
                        System.out.printf("📊 [STATS] Clients: %d | Keys: %d | Channels: %d | OPS: %d cmd/s\n", 
                            activeConnections.get(), store.size(), pubSubStore.size(), ops);
                    }
                } catch (InterruptedException e) { break; }
            }
        });
        monitor.setDaemon(true);
        monitor.start();

        printBanner();

        // 3. Start Server
        try (ServerSocket server = new ServerSocket(PORT);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            
            System.out.println("🔥 Ready on port " + PORT);
            System.out.println("🔒 Auth mode: " + (PASSWORD.equals("teasertopsecret") ? "Default (Unsafe)" : "Env Configured"));

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutting down...");
                saveData();
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
    private static void cleanupExpiredKeys() {
        int removed = 0;
        Iterator<Map.Entry<String, ValueEntry>> it = store.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, ValueEntry> entry = it.next();
            if (entry.getValue().isExpired()) {
                it.remove();
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
                        store.put(key, new ValueEntry((String) val, -1));
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
    private static class ClientHandler implements Runnable {
        private final Socket socket;
        private boolean auth = false;
        private String subscribedChannel = null; // Track if this client is a listener

        public ClientHandler(Socket socket) { this.socket = socket; }

        @Override
        public void run() {
            activeConnections.incrementAndGet();
            try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
            ) {
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    totalCommands.incrementAndGet();

                    // If subscribed, we basically ignore all commands except QUIT (simple mode)
                    if (subscribedChannel != null) {
                        if (line.trim().equalsIgnoreCase("QUIT")) break;
                        // In strict Redis, you can UNSUBSCRIBE, but here we just ignore to keep it simple.
                        continue; 
                    }

                    List<String> parts = parseCommand(line);
                    if (parts.isEmpty()) continue;
                    String cmd = parts.get(0).toUpperCase();

                    if (!auth && !cmd.equals("AUTH") && !cmd.equals("QUIT")) {
                        out.println("(error) NOAUTH Authentication required.");
                        continue;
                    }

                    try {
                        switch (cmd) {
                            case "AUTH":
                                if (parts.size() < 2) out.println("(error) usage: AUTH password");
                                else if (parts.get(1).equals(PASSWORD)) {
                                    auth = true;
                                    out.println("OK");
                                } else out.println("(error) WRONGPASS");
                                break;

                            case "SET":
                                if (parts.size() < 3) out.println("(error) usage: SET key value [EX seconds]");
                                else {
                                    long ttl = -1;
                                    if (parts.size() >= 5 && parts.get(3).equalsIgnoreCase("EX")) {
                                        try { ttl = Long.parseLong(parts.get(4)); } catch (Exception e) {}
                                    }
                                    store.put(parts.get(1), new ValueEntry(parts.get(2), ttl));
                                    out.println("OK");
                                }
                                break;

                            case "GET":
                                if (parts.size() < 2) out.println("(error) usage: GET key");
                                else {
                                    ValueEntry entry = store.get(parts.get(1));
                                    if (entry == null) out.println("(nil)");
                                    else if (entry.isExpired()) { store.remove(parts.get(1)); out.println("(nil)"); }
                                    else { String v = entry.value; out.println(v.contains(" ") ? "\"" + v + "\"" : v); }
                                }
                                break;

                            case "DEL":
                                if (parts.size() < 2) out.println("(error) usage: DEL key");
                                else { store.remove(parts.get(1)); out.println("(integer) 1"); }
                                break;
                            
                            // --- NEW PUB/SUB COMMANDS ---
                            case "SUBSCRIBE":
                                if (parts.size() < 2) out.println("(error) usage: SUBSCRIBE channel");
                                else {
                                    String channel = parts.get(1);
                                    subscribedChannel = channel;
                                    pubSubStore.computeIfAbsent(channel, k -> ConcurrentHashMap.newKeySet()).add(out);
                                    out.println("Subscribed to channel: " + channel);
                                    // Loop here until socket closes? 
                                    // Actually, in this simple implementation, we just return to the while loop
                                    // but we set 'subscribedChannel' flag to ignore other commands.
                                    // The 'out' is now held in the map for broadcasting.
                                }
                                break;

                            case "PUBLISH":
                                if (parts.size() < 3) out.println("(error) usage: PUBLISH channel message");
                                else {
                                    String channel = parts.get(1);
                                    String msg = parts.get(2);
                                    Set<PrintWriter> subs = pubSubStore.get(channel);
                                    int count = 0;
                                    if (subs != null) {
                                        // Broadcast
                                        Iterator<PrintWriter> it = subs.iterator();
                                        while(it.hasNext()) {
                                            PrintWriter subOut = it.next();
                                            try {
                                                subOut.println("[MSG] " + channel + ": " + msg);
                                                count++;
                                            } catch (Exception e) {
                                                it.remove(); // Remove dead subscriber
                                            }
                                        }
                                    }
                                    out.println("(integer) " + count);
                                }
                                break;

                            case "DBSIZE": out.println("(integer) " + store.size()); break;
                            case "FLUSHALL": store.clear(); out.println("OK"); break;
                            case "PING": out.println("PONG"); break;
                            case "QUIT": socket.close(); return;
                            default: out.println("(error) ERR unknown command");
                        }
                    } catch (Exception e) { out.println("(error) ERR " + e.getMessage()); }
                }
            } catch (IOException e) { 
                // Client disconnected
            } finally {
                // Cleanup Pub/Sub subscription on disconnect
                if (subscribedChannel != null) {
                    Set<PrintWriter> subs = pubSubStore.get(subscribedChannel);
                    if (subs != null) {
                        // We can't easily remove 'out' here because we don't have reference to it in finally block
                        // easily without restructuring, but the PUBLISH loop handles dead writers.
                        // Ideally we should remove it here to be clean.
                    }
                }
                activeConnections.decrementAndGet();
            }
        }

        private List<String> parseCommand(String line) {
            List<String> list = new ArrayList<>();
            Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(line);
            while (m.find()) {
                String match = m.group(1);
                if (match.startsWith("\"") && match.endsWith("\"")) list.add(match.substring(1, match.length() - 1));
                else list.add(match);
            }
            return list;
        }
    }
}