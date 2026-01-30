package core.db;

import core.Config;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class CaradeDatabaseConcurrencyTest {

    static class TestDatabase extends CaradeDatabase {
        public AtomicInteger expiredNotifications = new AtomicInteger(0);
        public AtomicInteger delNotifications = new AtomicInteger(0);

        public TestDatabase(Config config) {
            super(config, null);
        }

        @Override
        protected void notify(int dbIndex, String key, String event) {
            if (event.equals("expired")) {
                expiredNotifications.incrementAndGet();
            } else if (event.equals("del")) {
                delNotifications.incrementAndGet();
            }
        }
    }

    @Test
    public void testLazyExpirationRace() throws InterruptedException {
        Config config = new Config();
        TestDatabase db = new TestDatabase(config);
        
        // Setup: Key expired 1 second ago
        String key = "raceKey";
        ValueEntry val = new ValueEntry("value".getBytes(), DataType.STRING, System.currentTimeMillis() - 1000);
        db.put(0, key, val);
        
        int threadCount = 2;
        ExecutorService es = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(1);
        
        // Task: concurrent get
        Runnable task = () -> {
            try {
                latch.await(); // Sync start
                db.get(0, key);
            } catch (Exception e) {}
        };
        
        for (int i=0; i<threadCount; i++) es.submit(task);
        
        latch.countDown(); // Go!
        es.shutdown();
        es.awaitTermination(5, TimeUnit.SECONDS);
        
        // Verification
        // If race condition exists:
        // Both threads see isExpired() -> true
        // Thread A remove() -> returns Value, notifies "del"
        // Thread B remove() -> returns null (atomic ConcurrentHashMap), NO "del"
        // Thread A notifies "expired"
        // Thread B notifies "expired" (BUG)
        
        // We expect "expired" to be called exactly ONCE if logic is correct.
        // If bug exists, it will be 2.
        
        assertEquals(1, db.expiredNotifications.get(), "Should notify 'expired' exactly once");
        assertEquals(1, db.delNotifications.get(), "Should notify 'del' exactly once");
    }

    @Test
    public void testEvictionUnderLoad() throws InterruptedException {
        Config config = new Config();
        config.maxMemory = 1024 * 1024; // 1MB
        config.maxMemoryPolicy = "allkeys-lru";
        
        class EvictionDB extends CaradeDatabase {
            long fakeUsed = 0;
            public EvictionDB(Config c) { super(c, null); }
            @Override protected long getUsedMemory() { return fakeUsed; }
        }
        
        EvictionDB db = new EvictionDB(config);
        db.fakeUsed = 2000000; // > 1MB
        
        AtomicInteger successWrites = new AtomicInteger(0);
        
        // Thread 1: Writes (triggers eviction)
        Thread writer = new Thread(() -> {
            for(int i=0; i<1000; i++) {
                db.put(0, "k"+i, new ValueEntry("v".getBytes(), DataType.STRING, -1));
                successWrites.incrementAndGet();
                try { Thread.sleep(1); } catch (Exception e) {}
            }
        });
        
        // Thread 2: Reads
        Thread reader = new Thread(() -> {
             for(int i=0; i<1000; i++) {
                 db.get(0, "k"+i); // Might miss
                 try { Thread.sleep(1); } catch (Exception e) {}
             }
        });
        
        writer.start();
        reader.start();
        
        writer.join();
        reader.join();
        
        // Just ensuring no exceptions/deadlocks
        assertTrue(successWrites.get() > 0);
    }
}
