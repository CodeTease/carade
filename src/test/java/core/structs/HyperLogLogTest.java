package core.structs;

import core.Carade;
import core.commands.hll.PfAddCommand;
import core.commands.hll.PfCountCommand;
import core.commands.hll.PfMergeCommand;
import core.db.CaradeDatabase;
import core.network.ClientHandler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class HyperLogLogTest {

    static class MockClientHandler extends ClientHandler {
        public String lastResponse;
        public Long lastIntegerResponse;

        @Override
        public void sendResponse(byte[] respData, String raw) {
            this.lastResponse = raw;
        }

        @Override
        public void sendInteger(long value) {
            this.lastIntegerResponse = value;
            this.lastResponse = ":" + value + "\r\n";
        }
        
        @Override
        public void sendSimpleString(String msg) {
            this.lastResponse = msg; // Usually OK
        }
    }

    @BeforeEach
    public void setup() {
        Carade.db = CaradeDatabase.getInstance();
        CaradeDatabase.getInstance().clearAll();
    }

    private List<byte[]> makeArgs(String... args) {
        List<byte[]> list = new ArrayList<>();
        for (String s : args) {
            list.add(s.getBytes(StandardCharsets.UTF_8));
        }
        return list;
    }

    @Test
    public void testPfAddAndCount() {
        PfAddCommand pfAdd = new PfAddCommand();
        PfCountCommand pfCount = new PfCountCommand();
        MockClientHandler client = new MockClientHandler();

        // PFADD hll a b c
        pfAdd.execute(client, makeArgs("PFADD", "hll", "a", "b", "c"));
        assertEquals(1L, client.lastIntegerResponse); // 1 = modified

        // PFCOUNT hll -> should be 3
        pfCount.execute(client, makeArgs("PFCOUNT", "hll"));
        assertEquals(3L, client.lastIntegerResponse);

        // PFADD hll a (duplicate)
        pfAdd.execute(client, makeArgs("PFADD", "hll", "a"));
        assertEquals(0L, client.lastIntegerResponse); // 0 = not modified

        // PFCOUNT hll -> still 3
        pfCount.execute(client, makeArgs("PFCOUNT", "hll"));
        assertEquals(3L, client.lastIntegerResponse);
    }

    @Test
    public void testPfMerge() {
        PfAddCommand pfAdd = new PfAddCommand();
        PfMergeCommand pfMerge = new PfMergeCommand();
        PfCountCommand pfCount = new PfCountCommand();
        MockClientHandler client = new MockClientHandler();

        // hll1: a b c
        pfAdd.execute(client, makeArgs("PFADD", "hll1", "a", "b", "c"));
        // hll2: c d e
        pfAdd.execute(client, makeArgs("PFADD", "hll2", "c", "d", "e"));

        // PFMERGE hll_merged hll1 hll2
        // Distinct elements: a, b, c, d, e -> 5
        pfMerge.execute(client, makeArgs("PFMERGE", "hll_merged", "hll1", "hll2"));
        assertEquals("OK", client.lastResponse);

        pfCount.execute(client, makeArgs("PFCOUNT", "hll_merged"));
        assertEquals(5L, client.lastIntegerResponse);
    }

    @Test
    public void testPfMergeErrorRate() {
        PfAddCommand pfAdd = new PfAddCommand();
        PfMergeCommand pfMerge = new PfMergeCommand();
        PfCountCommand pfCount = new PfCountCommand();
        MockClientHandler client = new MockClientHandler();
        
        int numSets = 5;
        int itemsPerSet = 1000;
        
        for (int i = 0; i < numSets; i++) {
            List<byte[]> args = new ArrayList<>();
            args.add("PFADD".getBytes(StandardCharsets.UTF_8));
            args.add(("hll_" + i).getBytes(StandardCharsets.UTF_8));
            for (int j = 0; j < itemsPerSet; j++) {
                args.add(("val_" + i + "_" + j).getBytes(StandardCharsets.UTF_8));
            }
            pfAdd.execute(client, args);
        }
        
        List<byte[]> mergeArgs = new ArrayList<>();
        mergeArgs.add("PFMERGE".getBytes(StandardCharsets.UTF_8));
        mergeArgs.add("hll_final".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < numSets; i++) {
            mergeArgs.add(("hll_" + i).getBytes(StandardCharsets.UTF_8));
        }
        
        pfMerge.execute(client, mergeArgs);
        
        pfCount.execute(client, makeArgs("PFCOUNT", "hll_final"));
        long count = client.lastIntegerResponse;
        long expected = numSets * itemsPerSet; // 5000 unique items
        
        // HLL standard error is ~0.81%. Allow 5% tolerance here.
        double error = Math.abs(count - expected) / (double) expected;
        assertTrue(error < 0.05, "Error rate " + error + " too high for HLL merge (got " + count + ", expected " + expected + ")");
    }
}
