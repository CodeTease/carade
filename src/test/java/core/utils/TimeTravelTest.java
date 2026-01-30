package core.utils;

import core.db.CaradeDatabase;
import core.db.DataType;
import core.db.ValueEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.atomic.AtomicLong;

public class TimeTravelTest {
    
    private static class MockClock implements Time.Clock {
        private final AtomicLong currentTime = new AtomicLong(System.currentTimeMillis());
        
        @Override
        public long currentTimeMillis() {
            return currentTime.get();
        }
        
        public void advance(long millis) {
            currentTime.addAndGet(millis);
        }
    }
    
    @BeforeEach
    public void setup() {
        CaradeDatabase.getInstance().clearAll();
    }
    
    @AfterEach
    public void tearDown() {
        Time.useSystemClock();
    }

    @Test
    public void testExpirationWithTimeTravel() {
        MockClock mockClock = new MockClock();
        Time.setClock(mockClock);
        
        CaradeDatabase db = CaradeDatabase.getInstance();
        long now = Time.now();
        long expireAt = now + 1000;
        
        ValueEntry entry = new ValueEntry("test_val", DataType.STRING, expireAt);
        db.put("key1", entry);
        
        // Assert exists
        assertNotNull(db.get("key1"));
        
        // Advance time by 500ms
        mockClock.advance(500);
        assertNotNull(db.get("key1"));
        
        // Advance time by another 501ms (total 1001)
        mockClock.advance(501);
        
        // Should be expired
        assertNull(db.get("key1"));
    }
}
