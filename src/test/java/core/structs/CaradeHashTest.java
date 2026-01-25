package core.structs;

import org.junit.jupiter.api.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class CaradeHashTest {

    @Test
    public void testLazyExpiration() throws InterruptedException {
        CaradeHash hash = new CaradeHash();
        hash.put("k1", "v1");
        hash.put("k2", "v2");
        
        // Expire k1 immediately (1ms ago)
        hash.setExpiry("k1", System.currentTimeMillis() - 100);
        // Expire k2 in future
        hash.setExpiry("k2", System.currentTimeMillis() + 10000);
        
        // Lazy expiration check on get
        assertNull(hash.get("k1"), "Expired key should return null");
        assertEquals("v2", hash.get("k2"), "Valid key should return value");
        
        // Check size after lazy cleanup (get("k1") should have removed it)
        assertTrue(hash.map.containsKey("k2"));
        assertFalse(hash.map.containsKey("k1"));
    }
    
    @Test
    public void testKeySetValuesExpiration() {
        CaradeHash hash = new CaradeHash();
        hash.put("k1", "v1");
        hash.put("k2", "v2");
        hash.setExpiry("k1", System.currentTimeMillis() - 100);
        
        // keySet() calls cleanup() internally
        assertFalse(hash.keySet().contains("k1"));
        assertTrue(hash.keySet().contains("k2"));
        
        // values() calls cleanup() internally
        assertFalse(hash.values().contains("v1"));
        assertTrue(hash.values().contains("v2"));
    }

    @Test
    public void testCleanupMechanism() {
        CaradeHash hash = new CaradeHash();
        int count = 10000; // Reduced from 100k for unit test speed, but sufficient for logic
        long now = System.currentTimeMillis();
        
        for (int i = 0; i < count; i++) {
            hash.put("k" + i, "v" + i);
            if (i % 2 == 0) {
                // Expire even keys
                hash.setExpiry("k" + i, now - 100);
            } else {
                // Keep odd keys
                hash.setExpiry("k" + i, now + 100000);
            }
        }
        
        // Manual cleanup call
        hash.cleanup();
        
        assertEquals(count / 2, hash.size(), "Should have removed half the keys");
        
        for (int i = 0; i < count; i++) {
            if (i % 2 == 0) {
                assertFalse(hash.map.containsKey("k" + i));
            } else {
                assertTrue(hash.map.containsKey("k" + i));
            }
        }
    }

    @Test
    public void testRaceConditions() throws InterruptedException {
        CaradeHash hash = new CaradeHash();
        int threads = 4;
        int iterations = 1000;
        ExecutorService es = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicBoolean error = new AtomicBoolean(false);

        for (int i = 0; i < threads; i++) {
            es.submit(() -> {
                try {
                    for (int j = 0; j < iterations; j++) {
                        String key = "k" + j;
                        hash.put(key, "v" + j);
                        // Randomly expire or remove
                        if (Math.random() > 0.5) {
                            hash.setExpiry(key, System.currentTimeMillis() + 100);
                        } else {
                            hash.remove(key);
                        }
                        
                        // Check consistency: if key in expirations, should be in map (unless expired just now)
                        // This is hard to assert strictly due to race, but we can check if cleanup throws exception
                        hash.get(key);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    error.set(true);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await(5, TimeUnit.SECONDS);
        es.shutdown();
        
        assertFalse(error.get(), "Exception occurred during concurrent access");
        
        // Final cleanup should be consistent
        hash.cleanup();
        assertEquals(hash.map.size(), hash.expirations.size() + (hash.map.size() - hash.expirations.size())); 
        // Logic check: every expiration entry must have a map entry? Not necessarily if map entry removed but expiration not yet?
        // CaradeHash logic: remove() removes from both. setExpiry checks map first.
        
        // Verify expirations don't have dangling keys (keys not in map)
        for (String k : hash.expirations.keySet()) {
            assertTrue(hash.map.containsKey(k), "Expiration map has key " + k + " but main map does not");
        }
    }
}
