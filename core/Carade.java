import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Project: Carade
 * Version: 0.1.0 (The "Universe" Edition)
 * Status: Alpha / Dev / Chaos
 * Author: CodeTease Team
 * "We don't bump versions, we bump features."
 */
public class Carade {

    // --- CONFIGURATION ---
    private static final int PORT = 63790;
    private static final String DUMP_FILE = "carade.dump";
    private static final String DEFAULT_PASSWORD = "teasertopsecret"; 
    public static void printBanner() {
        System.out.println("\n" +
                "   ______                     __   \n" +
                "  / ____/___ ______________  / /__ \n" +
                " / /   / __ `/ ___/ __  / __  / _ \\\n" +
                "/ /___/ /_/ / /  / /_/ / /_/ /  __/\n" +
                "\\____/\\__,_/_/   \\__,_/\\__,_/\\___/ \n" +
                "                                   \n" +
                " :: Carade ::       (v0.1.0-alpha) \n" +
                " :: Engine ::       Java In-Memory \n");
    }
    
    // Core Storage
    private static ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
    
    // Server State
    private static boolean isRunning = true;

    public static void main(String[] args) {
        System.out.println("\n--- CARADE v0.1.0 (Stable, I think) ---\n");

        // 1. Load Data
        loadData();

        // 2. Auto-Save Background Thread (Daemon)
        Thread autoSave = new Thread(() -> {
            while (isRunning) {
                try {
                    Thread.sleep(30000); // Save every 30s
                    saveData();
                } catch (InterruptedException e) { break; }
            }
        });
        autoSave.setDaemon(true);
        autoSave.start();
        // Print ASCII Banner
        printBanner();
        // 3. Start Server
        ExecutorService threadPool = Executors.newCachedThreadPool(); // Unlimited power!

        try (ServerSocket server = new ServerSocket(PORT)) {
            System.out.println("🔥 Ready on port " + PORT);
            System.out.println("🔒 Auth required: " + DEFAULT_PASSWORD);
            
            // Add Shutdown Hook to save before kill
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n🛑 Shutting down...");
                saveData();
            }));

            while (isRunning) {
                Socket client = server.accept();
                threadPool.execute(new ClientHandler(client));
            }
        } catch (IOException e) {
            System.err.println("💥 Server crash: " + e.getMessage());
        }
    }

    // --- PERSISTENCE ---
    private static void saveData() {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(DUMP_FILE))) {
            oos.writeObject(store);
            System.out.println("💾 [AUTO-SAVE] " + store.size() + " keys saved.");
        } catch (IOException e) {
            System.err.println("⚠️ Save failed: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private static void loadData() {
        File f = new File(DUMP_FILE);
        if (!f.exists()) return;
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(f))) {
            store = (ConcurrentHashMap<String, String>) ois.readObject();
            System.out.println("📂 Loaded " + store.size() + " keys from disk.");
        } catch (Exception e) {
            System.out.println("⚠️ Dump file corrupted or empty. Starting fresh.");
        }
    }

    // --- HANDLER ---
    private static class ClientHandler implements Runnable {
        private final Socket socket;
        private boolean auth = false;

        public ClientHandler(Socket socket) { this.socket = socket; }

        @Override
        public void run() {
            try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true)
            ) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] parts = line.trim().split("\\s+", 3);
                    if (parts.length == 0 || parts[0].isEmpty()) continue;
                    
                    String cmd = parts[0].toUpperCase();

                    // Bypass Auth for AUTH/QUIT commands only
                    if (!auth && !cmd.equals("AUTH") && !cmd.equals("QUIT")) {
                        out.println("(error) NOAUTH Authentication required.");
                        continue;
                    }

                    switch (cmd) {
                        case "AUTH":
                            if (parts.length < 2) out.println("(error) usage: AUTH password");
                            else if (parts[1].equals(DEFAULT_PASSWORD)) {
                                auth = true;
                                out.println("OK");
                            } else out.println("(error) WRONGPASS");
                            break;

                        case "SET":
                            if (parts.length < 3) out.println("(error) usage: SET key value");
                            else {
                                store.put(parts[1], parts[2]);
                                out.println("OK");
                            }
                            break;

                        case "GET":
                            if (parts.length < 2) out.println("(error) usage: GET key");
                            else {
                                String v = store.get(parts[1]);
                                out.println(v != null ? v : "(nil)");
                            }
                            break;

                        case "DEL":
                             if (parts.length < 2) out.println("(error) usage: DEL key");
                             else {
                                 store.remove(parts[1]);
                                 out.println("(integer) 1");
                             }
                             break;
                        
                        case "DBSIZE":
                            out.println("(integer) " + store.size());
                            break;

                        case "PING": out.println("PONG"); break;
                        case "QUIT": socket.close(); return;
                        default: out.println("(error) ERR unknown command");
                    }
                }
            } catch (IOException e) { /* Client disconnected */ }
        }
    }
}