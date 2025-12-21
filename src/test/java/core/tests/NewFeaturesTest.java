package core.tests;

import core.Carade;
import core.commands.CommandRegistry;
import core.commands.hll.PfAddCommand;
import core.commands.hll.PfCountCommand;
import core.commands.string.BitCountCommand;
import core.commands.string.BitOpCommand;
import core.network.ClientHandler;
import core.structs.HyperLogLog;
import core.db.ValueEntry;
import core.db.DataType;
import core.server.SlowlogManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class NewFeaturesTest {
    
    private ClientHandler client;

    @BeforeEach
    public void setUp() {
        // Mocking Carade DB for standalone testing
        Carade.db = new core.db.CaradeDatabase(new core.Config(), null);
        Carade.config = new core.Config();
        client = new ClientHandler();
        client.dbIndex = 0;
        
        // Ensure commands are registered
        CommandRegistry.register("BITCOUNT", new BitCountCommand());
        CommandRegistry.register("BITOP", new BitOpCommand());
        CommandRegistry.register("PFADD", new PfAddCommand());
        CommandRegistry.register("PFCOUNT", new PfCountCommand());
    }

    private List<byte[]> toArgs(String... args) {
        List<byte[]> list = new ArrayList<>();
        for (String s : args) {
            list.add(s.getBytes(StandardCharsets.UTF_8));
        }
        return list;
    }

    @Test
    public void testBitCount() {
        String key = "mykey";
        byte[] data = new byte[] { (byte)0xFF, (byte)0x0F }; // 8 bits + 4 bits = 12 bits set
        Carade.db.put(0, key, new ValueEntry(data, DataType.STRING, -1));
        
        BitCountCommand cmd = new BitCountCommand();
        
        final long[] result = {-1};
        client = new ClientHandler() {
            @Override
            public void sendInteger(long i) {
                result[0] = i;
            }
        };
        client.dbIndex = 0;
        
        cmd.execute(client, toArgs("BITCOUNT", key));
        assertEquals(12, result[0]);
    }

    @Test
    public void testSlowlog() {
         SlowlogManager.clear();
         SlowlogManager.log("test log");
         assertEquals(1, SlowlogManager.size());
         assertEquals("test log", SlowlogManager.getLogs().get(0));
    }
}
