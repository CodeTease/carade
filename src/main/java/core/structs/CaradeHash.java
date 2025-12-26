package core.structs;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CaradeHash implements Serializable {
    public final ConcurrentHashMap<String, String> map;
    public final ConcurrentHashMap<String, Long> expirations;

    public CaradeHash() {
        this.map = new ConcurrentHashMap<>();
        this.expirations = new ConcurrentHashMap<>();
    }
    
    // For conversion from existing maps
    public CaradeHash(ConcurrentHashMap<String, String> data) {
        this.map = data;
        this.expirations = new ConcurrentHashMap<>();
    }

    public String put(String field, String value) {
        return map.put(field, value);
    }

    public String get(String field) {
        checkExpiry(field);
        return map.get(field);
    }
    
    public String remove(String field) {
        expirations.remove(field);
        return map.remove(field);
    }
    
    public int size() {
        cleanup();
        return map.size();
    }
    
    public boolean isEmpty() {
        cleanup();
        return map.isEmpty();
    }
    
    public void setExpiry(String field, long timestamp) {
        if (map.containsKey(field)) {
            expirations.put(field, timestamp);
        }
    }
    
    public long getExpiry(String field) {
        checkExpiry(field);
        return expirations.getOrDefault(field, -1L);
    }
    
    public void removeExpiry(String field) {
        expirations.remove(field);
    }
    
    private void checkExpiry(String field) {
        Long expireAt = expirations.get(field);
        if (expireAt != null && System.currentTimeMillis() > expireAt) {
            map.remove(field);
            expirations.remove(field);
        }
    }
    
    public void cleanup() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, Long>> it = expirations.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Long> entry = it.next();
            if (now > entry.getValue()) {
                map.remove(entry.getKey());
                it.remove();
            }
        }
    }
    
    public Set<Map.Entry<String, String>> entrySet() {
        cleanup();
        return map.entrySet();
    }
    
    public Set<String> keySet() {
        cleanup();
        return map.keySet();
    }
    
    public Collection<String> values() {
        cleanup();
        return map.values();
    }
}
