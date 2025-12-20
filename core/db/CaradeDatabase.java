package core.db;

import core.Config;
import core.persistence.AofHandler;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CaradeDatabase {
    // Array of databases
    public static final int DB_COUNT = 16;
    public final ConcurrentHashMap<String, ValueEntry>[] databases;
    
    // Legacy public access to store (mapped to DB 0 for backward compatibility if accessed directly, 
    // but we should try to avoid direct access)
    // Deprecated: Use getStorage(int dbIndex) instead
    public final ConcurrentHashMap<String, ValueEntry> store; 

    private static CaradeDatabase INSTANCE;
    
    public static synchronized CaradeDatabase getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new CaradeDatabase(new Config(), AofHandler.getInstance());
        }
        return INSTANCE;
    }

    private final Config config;
    private final AofHandler aofHandler;
    private final AtomicInteger writeCounter = new AtomicInteger(0);

    @SuppressWarnings("unchecked")
    public CaradeDatabase(Config config, AofHandler aofHandler) {
        this.config = config;
        this.aofHandler = aofHandler;
        this.databases = new ConcurrentHashMap[DB_COUNT];
        for (int i = 0; i < DB_COUNT; i++) {
            this.databases[i] = new ConcurrentHashMap<>();
        }
        this.store = this.databases[0]; // Default to DB 0 for legacy access
        INSTANCE = this;
    }

    public ConcurrentHashMap<String, ValueEntry> getStore(int dbIndex) {
        if (dbIndex < 0 || dbIndex >= DB_COUNT) throw new IllegalArgumentException("DB index out of range");
        return databases[dbIndex];
    }

    public ValueEntry get(int dbIndex, String key) {
        ConcurrentHashMap<String, ValueEntry> db = getStore(dbIndex);
        ValueEntry v = db.get(key);
        if (v != null) {
            if (v.isExpired()) {
                remove(dbIndex, key);
                return null;
            }
            v.touch();
        }
        return v;
    }
    
    // Overload for default DB 0
    public ValueEntry get(String key) {
        return get(0, key);
    }

    public void put(int dbIndex, String key, ValueEntry value) {
        performEvictionIfNeeded(dbIndex);
        getStore(dbIndex).put(key, value);
    }
    
    // Overload for default DB 0
    public void put(String key, ValueEntry value) {
        put(0, key, value);
    }
    
    public ValueEntry remove(int dbIndex, String key) {
        ValueEntry v = getStore(dbIndex).remove(key);
        // AOF logging is handled at command level usually
        return v;
    }
    
    // Overload for default DB 0
    public ValueEntry remove(String key) {
        return remove(0, key);
    }
    
    public void performEvictionIfNeeded(int dbIndex) {
        if (config.maxMemory <= 0) return;
        if (writeCounter.incrementAndGet() % 50 != 0) return;
        long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (used < config.maxMemory) return;
        
        System.out.println("🧹 [Eviction][DB"+dbIndex+"] Memory full (" + (used/1024/1024) + "MB > " + (config.maxMemory/1024/1024) + "MB). Evicting...");
        
        ConcurrentHashMap<String, ValueEntry> db = getStore(dbIndex);
        int attempts = 0;
        Iterator<String> it = db.keySet().iterator();
        while (used > config.maxMemory && !db.isEmpty() && attempts < 100) {
            String bestKey = null;
            long oldestTime = Long.MAX_VALUE;
            
            int samples = 0;
            while (it.hasNext() && samples < 5) {
                String key = it.next();
                ValueEntry v = db.get(key);
                if (v != null) {
                    if (v.lastAccessed < oldestTime) {
                        oldestTime = v.lastAccessed;
                        bestKey = key;
                    }
                }
                samples++;
            }
            if (!it.hasNext()) it = db.keySet().iterator();
            
            if (bestKey != null) {
                db.remove(bestKey);
                if (aofHandler != null) aofHandler.log("DEL", bestKey); // Note: AOF might need SELECT context
            }
            
            used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            attempts++;
        }
    }
    
    // Default eviction on DB 0
    public void performEvictionIfNeeded() {
        performEvictionIfNeeded(0);
    }
    
    public int size(int dbIndex) {
        return getStore(dbIndex).size();
    }
    
    // Default size DB 0
    public int size() {
        return size(0);
    }
    
    public void clear(int dbIndex) {
        getStore(dbIndex).clear();
    }
    
    // Default clear DB 0
    public void clear() {
        clear(0);
    }
    
    public void clearAll() {
        for(int i=0; i<DB_COUNT; i++) {
            databases[i].clear();
        }
    }
    
    public ConcurrentHashMap<String, ValueEntry> getStorage(int dbIndex) {
        return getStore(dbIndex);
    }

    public boolean exists(int dbIndex, String key) {
        return get(dbIndex, key) != null;
    }

    public void cleanup() {
         long now = System.currentTimeMillis();
         for(int i=0; i<DB_COUNT; i++) {
             databases[i].values().removeIf(v -> v.isExpired(now));
         }
    }
    
    public Set<String> keySet(int dbIndex) {
        return getStore(dbIndex).keySet();
    }
    
    // Default keySet DB 0
    public Set<String> keySet() {
        return keySet(0);
    }
    
    public Set<Map.Entry<String, ValueEntry>> entrySet(int dbIndex) {
        return getStore(dbIndex).entrySet();
    }
    
    // Default entrySet DB 0
    public Set<Map.Entry<String, ValueEntry>> entrySet() {
        return entrySet(0);
    }
}
