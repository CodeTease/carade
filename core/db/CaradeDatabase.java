package core.db;

import core.Config;
import core.persistence.AofHandler;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CaradeDatabase {
    // Single instance for now, or per-DB instance
    public final ConcurrentHashMap<String, ValueEntry> store = new ConcurrentHashMap<>();
    
    // We can use locks here or rely on ConcurrentHashMap for most things.
    // Carade.java uses globalRWLock for some operations.
    // For now we expose store directly to ease migration, but ideally methods should be here.

    private final Config config;
    private final AofHandler aofHandler;
    private final AtomicInteger writeCounter = new AtomicInteger(0);

    public CaradeDatabase(Config config, AofHandler aofHandler) {
        this.config = config;
        this.aofHandler = aofHandler;
    }

    public ValueEntry get(String key) {
        ValueEntry v = store.get(key);
        if (v != null) {
            if (v.isExpired()) {
                remove(key);
                return null;
            }
            v.touch();
        }
        return v;
    }

    public void put(String key, ValueEntry value) {
        performEvictionIfNeeded();
        store.put(key, value);
    }
    
    public ValueEntry remove(String key) {
        ValueEntry v = store.remove(key);
        if (v != null && aofHandler != null) {
             // Logic for AOF logging is currently in Carade.java's switch case mostly
             // But for expiration/eviction it is handled internally
        }
        return v;
    }
    
    public void performEvictionIfNeeded() {
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
    
    public int size() {
        return store.size();
    }
    
    public void clear() {
        store.clear();
    }
    
    public Set<String> keySet() {
        return store.keySet();
    }
    
    public Set<Map.Entry<String, ValueEntry>> entrySet() {
        return store.entrySet();
    }
}
