package core.db;

import core.Config;
import core.persistence.CommandLogger;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import core.Carade; 

public class CaradeDatabase {
    // Array of databases
    public static final int DB_COUNT = 16;
    public final ConcurrentHashMap<String, ValueEntry>[] databases;
    
    public final ConcurrentHashMap<String, ValueEntry> store; 

    private static CaradeDatabase INSTANCE;
    
    public static synchronized CaradeDatabase getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new CaradeDatabase(new Config(), CommandLogger.getInstance());
        }
        return INSTANCE;
    }

    private final Config config;
    private final CommandLogger aofHandler;
    private final AtomicInteger writeCounter = new AtomicInteger(0);

    @SuppressWarnings("unchecked")
    public CaradeDatabase(Config config, CommandLogger aofHandler) {
        this.config = config;
        this.aofHandler = aofHandler;
        this.databases = new ConcurrentHashMap[DB_COUNT];
        for (int i = 0; i < DB_COUNT; i++) {
            this.databases[i] = new ConcurrentHashMap<>();
        }
        this.store = this.databases[0]; 
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
                notify(dbIndex, key, "expired");
                Carade.keyspaceMisses.incrementAndGet();
                return null;
            }
            v.touch();
            Carade.keyspaceHits.incrementAndGet();
        } else {
            Carade.keyspaceMisses.incrementAndGet();
        }
        return v;
    }
    
    public ValueEntry get(String key) {
        return get(0, key);
    }

    public void put(int dbIndex, String key, ValueEntry value) {
        performEvictionIfNeeded(dbIndex);
        boolean exists = getStore(dbIndex).containsKey(key);
        getStore(dbIndex).put(key, value);
        notify(dbIndex, key, exists ? "set" : "new"); 
    }
    
    public void put(String key, ValueEntry value) {
        put(0, key, value);
    }
    
    public ValueEntry remove(int dbIndex, String key) {
        ValueEntry v = getStore(dbIndex).remove(key);
        if (v != null) {
            notify(dbIndex, key, "del");
        }
        return v;
    }
    
    public ValueEntry remove(String key) {
        return remove(0, key);
    }
    
    private void notify(int dbIndex, String key, String event) {
        try {
            if (Carade.pubSub == null) return; 
            String channelKey = "__keyspace@" + dbIndex + "__:" + key;
            String channelEvent = "__keyevent@" + dbIndex + "__:" + event;
            Carade.pubSub.publish(channelKey, event);
            Carade.pubSub.publish(channelEvent, key);
        } catch (Exception e) {}
    }
    
    public void performEvictionIfNeeded(int dbIndex) {
        if (config.maxMemory <= 0) return;
        if (writeCounter.incrementAndGet() % 50 != 0) return;
        long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        if (used < config.maxMemory) return;
        
        String policy = config.maxMemoryPolicy;
        if (policy.equals("noeviction")) return;
        
        System.out.println("🧹 [Eviction][DB"+dbIndex+"] Policy: " + policy + ". Usage: " + (used/1024/1024) + "MB");
        
        ConcurrentHashMap<String, ValueEntry> db = getStore(dbIndex);
        int attempts = 0;
        Iterator<String> it = db.keySet().iterator();
        
        while (used > config.maxMemory && !db.isEmpty() && attempts < 100) {
            String bestKey = null;
            
            if (policy.contains("random")) {
                if (it.hasNext()) bestKey = it.next();
                if (policy.contains("volatile")) {
                    ValueEntry v = db.get(bestKey);
                    if (v == null || v.expireAt == -1) bestKey = null;
                }
            } else {
                long oldestTime = Long.MAX_VALUE;
                int samples = 0;
                while (it.hasNext() && samples < 5) {
                    String key = it.next();
                    ValueEntry v = db.get(key);
                    if (v != null) {
                        boolean eligible = true;
                        if (policy.contains("volatile") && v.expireAt == -1) eligible = false;
                        
                        if (eligible && v.lastAccessed < oldestTime) {
                            oldestTime = v.lastAccessed;
                            bestKey = key;
                        }
                    }
                    samples++;
                }
            }
            
            if (!it.hasNext()) it = db.keySet().iterator(); 
            
            if (bestKey != null) {
                db.remove(bestKey);
                notify(dbIndex, bestKey, "evicted");
                if (aofHandler != null) aofHandler.log("DEL", bestKey); 
            }
            
            used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            attempts++;
        }
    }
    
    public void performEvictionIfNeeded() {
        performEvictionIfNeeded(0);
    }
    
    public int size(int dbIndex) {
        return getStore(dbIndex).size();
    }
    
    public int size() {
        return size(0);
    }
    
    public void clear(int dbIndex) {
        getStore(dbIndex).clear();
    }
    
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
             Iterator<Map.Entry<String, ValueEntry>> it = databases[i].entrySet().iterator();
             while (it.hasNext()) {
                 Map.Entry<String, ValueEntry> e = it.next();
                 if (e.getValue().isExpired(now)) {
                     it.remove();
                     notify(i, e.getKey(), "expired");
                 }
             }
         }
    }
    
    public Set<String> keySet(int dbIndex) {
        return getStore(dbIndex).keySet();
    }
    
    public Set<String> keySet() {
        return keySet(0);
    }
    
    public Set<Map.Entry<String, ValueEntry>> entrySet(int dbIndex) {
        return getStore(dbIndex).entrySet();
    }
    
    public Set<Map.Entry<String, ValueEntry>> entrySet() {
        return entrySet(0);
    }
}
