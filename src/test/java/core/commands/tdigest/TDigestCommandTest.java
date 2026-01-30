package core.commands.tdigest;

import core.Carade;
import core.MockClientHandler;
import core.db.CaradeDatabase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class TDigestCommandTest {

    @BeforeEach
    public void setup() {
        if (Carade.db == null) {
            Carade.db = CaradeDatabase.getInstance();
        }
        Carade.db.clearAll();
    }

    @Test
    public void testTDigestAccuracy() {
        TdAddCommand addCmd = new TdAddCommand();
        MockClientHandler client = new MockClientHandler();
        
        // Add 1000 numbers: 1 to 1000
        List<byte[]> args = new ArrayList<>();
        args.add("TD.ADD".getBytes(StandardCharsets.UTF_8));
        args.add("digest".getBytes(StandardCharsets.UTF_8));
        
        for (int i = 1; i <= 1000; i++) {
            args.add(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }
        
        addCmd.execute(client, args);
        assertEquals("OK", client.lastResponse);
        
        // Check Quantile 0.5 (Median) - Should be close to 500.5
        TdQuantileCommand quantileCmd = new TdQuantileCommand();
        List<byte[]> argsQ = new ArrayList<>();
        argsQ.add("TD.QUANTILE".getBytes(StandardCharsets.UTF_8));
        argsQ.add("digest".getBytes(StandardCharsets.UTF_8));
        argsQ.add("0.5".getBytes(StandardCharsets.UTF_8));
        
        quantileCmd.execute(client, argsQ);
        assertNotNull(client.lastResponse);
        // lastResponse is "[500.5]" because sendArray was used
        String valStr = client.lastResponse.replace("[", "").replace("]", "");
        double median = Double.parseDouble(valStr);
        
        // T-Digest is probabilistic, but for uniform distribution it's usually good.
        // Allow 5% error margin or just check it's reasonable
        assertTrue(median > 450 && median < 550, "Median should be around 500, got: " + median);
    }
    
    @Test
    public void testTDigestCDF() {
        TdAddCommand addCmd = new TdAddCommand();
        MockClientHandler client = new MockClientHandler();
        
        // Add 1, 2, 3, 4, 5
        List<byte[]> args = new ArrayList<>();
        args.add("TD.ADD".getBytes(StandardCharsets.UTF_8));
        args.add("cdf_test".getBytes(StandardCharsets.UTF_8));
        for (int i = 1; i <= 5; i++) {
            args.add(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
        }
        addCmd.execute(client, args);
        
        // CDF(3) should be roughly 0.5 (rank of 3 in 1..5 is middle depending on interpolation)
        TdCdfCommand cdfCmd = new TdCdfCommand();
        List<byte[]> argsC = new ArrayList<>();
        argsC.add("TD.CDF".getBytes(StandardCharsets.UTF_8));
        argsC.add("cdf_test".getBytes(StandardCharsets.UTF_8));
        argsC.add("3".getBytes(StandardCharsets.UTF_8));
        
        cdfCmd.execute(client, argsC);
        String valStr = client.lastResponse.replace("[", "").replace("]", "");
        double cdf = Double.parseDouble(valStr);
        assertTrue(cdf > 0.3 && cdf < 0.7, "CDF of 3 in 1..5 should be around 0.5");
    }
}
