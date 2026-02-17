package core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.*;
import java.util.*;

public class Config {
    // Backward compat for ConfigGet
    public static final int PORT = 63790; 
    public static final String MAXMEMORY = "0"; 

    public String version = "0.3.4";
    public int port = 63790;
    public String password = "teasertopsecret";
    public long maxMemory = 268435456; // 256MB default
    public String maxMemoryPolicy = "noeviction"; 
    public Map<String, User> users = new HashMap<>();

    public Config() {
        // Default constructor for Jackson
        this.users.put("default", new User("default", this.password, true, true));
    }

    public static class User {
        public String name;
        public String password;
        public boolean isAdmin;
        public boolean canWrite;

        public User() { }

        public User(String name, String password, boolean isAdmin, boolean canWrite) {
            this.name = name;
            this.password = password;
            this.isAdmin = isAdmin;
            this.canWrite = canWrite;
        }
    }

    public static Config load(String filename) {
        // Try loading from YAML
        File f = new File(filename);
        if (!f.exists()) {
            // Try looking for .yaml extension if .conf was passed
            if (filename.endsWith(".conf")) {
                File yamlFile = new File(filename.replace(".conf", ".yaml"));
                if (yamlFile.exists()) f = yamlFile;
            }
        }

        Config config = new Config();

        if (!f.exists()) {
             System.out.println("⚠️ Config file not found: " + filename + ". Using defaults.");
             return config;
        }

        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            config = mapper.readValue(f, Config.class);
            
            if (config.users == null) config.users = new HashMap<>();
            
            if (!config.users.containsKey("default")) {
                 config.users.put("default", new User("default", config.password, true, true));
            }
            
        } catch (Exception e) {
            System.out.println("⚠️ Failed to load config as YAML (" + e.getMessage() + "). Attempting legacy parse...");
            config = loadLegacy(f, config);
        }

        if (System.getenv("CARADE_VERSION") != null) {
            config.version = System.getenv("CARADE_VERSION");
        }

        return config;
    }
    
    private static Config loadLegacy(File f, Config config) {
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
                    case "maxmemory-policy": config.maxMemoryPolicy = val; break;
                    case "user":
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
            System.out.println("✅ Loaded legacy config.");
        } catch (Exception e) {
            System.err.println("⚠️ Error loading legacy config: " + e.getMessage());
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
