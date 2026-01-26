package core.db;

import core.Config;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class CaradeDatabaseEvictionTest {

    static class TestableCaradeDatabase extends CaradeDatabase {
        private long mockedUsedMemory;

        public TestableCaradeDatabase(Config config) {
            super(config, null);
        }

        public void setMockedUsedMemory(long memory) {
            this.mockedUsedMemory = memory;
        }

        @Override
        protected long getUsedMemory() {
            return mockedUsedMemory;
        }

        @Override
        protected void notify(int dbIndex, String key, String event) {
            super.notify(dbIndex, key, event);
            if (event.equals("evicted")) {
                mockedUsedMemory -= 110; // Simulate dropping below 100 immediately after one eviction
            }
        }
    }

    @Test
    public void testAllKeysLRU() {
        Config config = new Config();
        config.maxMemory = 100;
        config.maxMemoryPolicy = "allkeys-lru";

        TestableCaradeDatabase db = new TestableCaradeDatabase(config);
        
        // Add 3 keys
        ValueEntry v1 = new ValueEntry("val1".getBytes(), DataType.STRING, -1);
        v1.lastAccessed = 1000;
        db.put(0, "k1", v1);
        
        ValueEntry v2 = new ValueEntry("val2".getBytes(), DataType.STRING, -1);
        v2.lastAccessed = 2000; // More recent
        db.put(0, "k2", v2);
        
        ValueEntry v3 = new ValueEntry("val3".getBytes(), DataType.STRING, -1);
        v3.lastAccessed = 500; // Oldest
        db.put(0, "k3", v3);

        // Force eviction
        db.setMockedUsedMemory(200); // > 100
        
        for(int i=0; i<60; i++) {
            db.performEvictionIfNeeded(0);
        }
        
        // Assert k3 is gone (LRU)
        assertFalse(db.exists(0, "k3"), "Oldest key k3 should be evicted");
        assertTrue(db.exists(0, "k1"), "k1 should stay");
        assertTrue(db.exists(0, "k2"), "k2 should stay");
    }

    @Test
    public void testVolatileLRU() {
        Config config = new Config();
        config.maxMemory = 100;
        config.maxMemoryPolicy = "volatile-lru"; // Only expire keys

        TestableCaradeDatabase db = new TestableCaradeDatabase(config);
        
        // k1: No expire, very old
        ValueEntry v1 = new ValueEntry("val1".getBytes(), DataType.STRING, -1);
        v1.lastAccessed = 100; 
        db.put(0, "k1", v1);
        
        // k2: Expire set, newer
        ValueEntry v2 = new ValueEntry("val2".getBytes(), DataType.STRING, System.currentTimeMillis() + 10000);
        v2.lastAccessed = 200; 
        db.put(0, "k2", v2);
        
        db.setMockedUsedMemory(200);
        
        for(int i=0; i<60; i++) db.performEvictionIfNeeded(0);
        
        // k2 should be evicted (volatile), k1 should stay (even if older)
        assertFalse(db.exists(0, "k2"), "Volatile key k2 should be evicted");
        assertTrue(db.exists(0, "k1"), "Persistent key k1 should NOT be evicted even if old");
    }
}
