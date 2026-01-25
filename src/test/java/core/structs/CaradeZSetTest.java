package core.structs;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class CaradeZSetTest {

    @Test
    public void testScoreUpdate() {
        CaradeZSet zset = new CaradeZSet();
        
        // Add A: 10
        zset.add(10.0, "A");
        assertEquals(1, zset.size());
        assertEquals(10.0, zset.score("A"));
        
        // Update A: 20
        zset.add(20.0, "A");
        assertEquals(1, zset.size()); // Size should remain 1
        assertEquals(20.0, zset.score("A")); // Score updated
        
        // Check sorted structure (iterator)
        ZNode first = zset.sorted.first();
        assertEquals("A", first.member);
        assertEquals(20.0, first.score);
        
        // Ensure no ghost node at 10.0
        assertEquals(1, zset.sorted.size());
    }
    
    @Test
    public void testBoundaries() {
        CaradeZSet zset = new CaradeZSet();
        zset.add(Double.POSITIVE_INFINITY, "inf");
        zset.add(Double.NEGATIVE_INFINITY, "-inf");
        zset.add(0.0, "zero");
        
        assertEquals(3, zset.size());
        assertEquals(Double.POSITIVE_INFINITY, zset.score("inf"));
        
        // Range check
        assertEquals(3, zset.rangeByScore(Double.NEGATIVE_INFINITY, true, Double.POSITIVE_INFINITY, true).size());
    }
}
