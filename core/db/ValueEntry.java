package core.db;

import java.io.Serializable;

public class ValueEntry implements Serializable {
    private static final long serialVersionUID = 2L;
    
    public Object value;
    public DataType type;
    public long expireAt;
    public long lastAccessed;

    public ValueEntry(Object value, DataType type, long ttlSeconds) {
        this.value = value;
        this.type = type;
        this.expireAt = ttlSeconds > 0 ? System.currentTimeMillis() + (ttlSeconds * 1000) : -1;
        this.lastAccessed = System.nanoTime();
    }

    public boolean isExpired() {
        return expireAt != -1 && System.currentTimeMillis() > expireAt;
    }

    public boolean isExpired(long now) {
        return expireAt != -1 && now > expireAt;
    }

    public void setExpireAt(long expireAt) {
        this.expireAt = expireAt;
    }

    public long getExpireAt() {
        return expireAt;
    }

    public void touch() {
        this.lastAccessed = System.nanoTime();
    }
}
