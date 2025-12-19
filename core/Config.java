package core;

import java.io.*;
import java.util.*;

public class Config {
    public static final int PORT = 63790; // Backward compat for ConfigGet
    public static final String MAXMEMORY = "0"; // Backward compat for ConfigGet

    public int port = 63790;
    public String password = "teasertopsecret";
    public long maxMemory = 0; // 0 = unlimited
    public Map<String, User> users = new HashMap<>();

    public static class User {
        public String name;
        public String password; // Plain text for simplicity, or hash?
        public boolean isAdmin;
        public boolean canWrite;

        public User(String name, String password, boolean isAdmin, boolean canWrite) {
            this.name = name;
            this.password = password;
            this.isAdmin = isAdmin;
            this.canWrite = canWrite;
        }
    }

    public static Config load(String filename) {
        Config config = new Config();
        // Default user
        config.users.put("default", new User("default", config.password, true, true));
        
        File f = new File(filename);
        if (!f.exists()) return config;

        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] parts = line.split("\\s+", 2);
                if (parts.length < 2) continue;
                
                String key = parts[0];
                String val = parts[1];
                
                switch (key) {
                    case "port": config.port = Integer.parseInt(val); break;
                    case "requirepass": 
                        config.password = val; 
                        config.users.get("default").password = val;
                        break;
                    case "maxmemory": config.maxMemory = parseMemory(val); break;
                    case "user":
                        // format: user <name> <password> <admin|readwrite|readonly>
                        String[] uParts = val.split("\\s+");
                        if (uParts.length >= 2) {
                            String name = uParts[0];
                            String pass = uParts[1];
                            String role = uParts.length > 2 ? uParts[2] : "readonly";
                            boolean isAdmin = role.equalsIgnoreCase("admin");
                            boolean canWrite = isAdmin || role.equalsIgnoreCase("readwrite");
                            config.users.put(name, new User(name, pass, isAdmin, canWrite));
                        }
                        break;
                }
            }
        } catch (Exception e) {
            System.err.println("⚠️ Error loading config: " + e.getMessage());
        }
        return config;
    }
    
    private static long parseMemory(String val) {
        val = val.toUpperCase();
        long factor = 1;
        if (val.endsWith("GB")) { factor = 1024*1024*1024L; val = val.replace("GB", ""); }
        else if (val.endsWith("MB")) { factor = 1024*1024L; val = val.replace("MB", ""); }
        else if (val.endsWith("KB")) { factor = 1024L; val = val.replace("KB", ""); }
        try {
            return Long.parseLong(val.trim()) * factor;
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
