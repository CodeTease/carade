package core.db;

import core.Carade;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class ExpirationTest {

    @BeforeEach
    public void setup() {
        if (Carade.db == null) {
            Carade.db = CaradeDatabase.getInstance();
        }
        Carade.db.clearAll();
    }

    @Test
    public void testPassiveExpiration() throws InterruptedException {
        String key = "exp_key";
        ValueEntry val = new ValueEntry("val".getBytes(StandardCharsets.UTF_8), DataType.STRING, -1);
        
        // Set expiry to 100ms from now
        val.expireAt = System.currentTimeMillis() + 100;
        
        Carade.db.put(0, key, val);
        
        // Immediately check: should exist
        assertNotNull(Carade.db.get(0, key));
        
        // Wait 200ms
        Thread.sleep(200);
        
        // Check again: should be null (passive expire on access)
        assertNull(Carade.db.get(0, key), "Key should have expired");
    }

    @Test
    public void testActiveExpirationManual() throws InterruptedException {
        String key = "active_exp_key";
        ValueEntry val = new ValueEntry("val".getBytes(StandardCharsets.UTF_8), DataType.STRING, -1);
        val.expireAt = System.currentTimeMillis() + 100;
        
        Carade.db.put(0, key, val);
        
        Thread.sleep(200);
        
        // Run cleanup (Active Expiration)
        Carade.db.cleanup();
        
        // Check raw storage: key should be gone
        ValueEntry entry = Carade.db.getStore(0).get(key);
        assertNull(entry, "Key should have been removed by active expiration");
    }
}
