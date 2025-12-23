package core;

import core.structs.tdigest.TDigest;
import org.junit.Test;
import static org.junit.Assert.*;

public class TDigestTest {

    @Test
    public void testTDigestBasic() {
        TDigest td = new TDigest(100);
        
        // Add uniform distribution 1..1000
        for (int i = 1; i <= 1000; i++) {
            td.add(i);
        }
        
        // Verify median ~ 500
        double median = td.quantile(0.5);
        System.out.println("Median (expected ~500): " + median);
        assertEquals(500, median, 10.0); // Allow some error
        
        // Verify p99 ~ 990
        double p99 = td.quantile(0.99);
        System.out.println("P99 (expected ~990): " + p99);
        assertEquals(990, p99, 10.0);
        
        // Verify p01 ~ 10
        double p01 = td.quantile(0.01);
        System.out.println("P01 (expected ~10): " + p01);
        assertEquals(10, p01, 10.0);
        
        // Check size
        assertEquals(1000, td.size());
    }
    
    @Test
    public void testTDigestSkewed() {
        TDigest td = new TDigest(100);
        
        // Add 900 zeros and 100 ones
        for(int i=0; i<900; i++) td.add(0);
        for(int i=0; i<100; i++) td.add(1);
        
        assertEquals(0.0, td.quantile(0.5), 0.01);
        assertEquals(0.0, td.quantile(0.89), 0.01);
        assertEquals(1.0, td.quantile(0.91), 0.01);
    }
}
